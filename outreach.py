import re
import os
import csv
import json
import datetime
import traceback
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
from linkedin_scraper import fetch_job_details, fetch_public_profile
from utils import call_llm
import prompts

CONFIG = {
    'job_url': None,  # Can be a single URL (str) or a list of URLs
    'cv_file': 'cv.txt',
    # Parallelization controls
    'batch_size': 5,
    'max_workers': 5,  # real workers = min(batch_size, max_workers)
}


def ensure_dirs():
    os.makedirs(config.OUTREACH_OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.STATE_DIR, exist_ok=True)


def load_processed_ids() -> set:
    ensure_dirs()
    path = config.OUTREACH_PROCESSED_IDS_PATH
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, list):
                return set(data)
            if isinstance(data, dict):
                acc = set()
                for _, ids in data.items():
                    if isinstance(ids, list):
                        acc.update(ids)
                return acc
            return set()
        except Exception as e:
            print(f"ERROR load_processed_ids: {e}")
            print(traceback.format_exc())
            return set()
    return set()


def append_run_processed_ids(run_ts: str, ids: set):
    ensure_dirs()
    existing = {}
    path = config.OUTREACH_PROCESSED_IDS_PATH
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                existing = data
            elif isinstance(data, list):
                existing = {"legacy": data}
        except Exception as e:
            print(f"ERROR reading processed ids: {e}")
            print(traceback.format_exc())
            existing = {}
    existing[run_ts] = sorted(list(ids))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(existing, f)


def extract_job_id(url: str) -> str:
    # Try currentJobId param
    m = re.search(r'[?&]currentJobId=(\d+)', url)
    if m:
        return m.group(1)
    # Try /view/<id>
    m = re.search(r'/view/(\d+)', url)
    if m:
        return m.group(1)
    # Fallback: any long number in URL
    m = re.search(r'(\d{8,})', url)
    if m:
        return m.group(1)
    raise ValueError('Could not extract job id from URL')


def read_cv_text(cv_path: str) -> str:
    with open(cv_path, 'r', encoding='utf-8') as f:
        return f.read()


def build_system_prompt_outreach(cv_text: str) -> str:
    return prompts.OUTREACH_SYSTEM_PROMPT.format(cv_text=cv_text)


def build_user_prompt_outreach(job: dict, profile: dict) -> str:
    bullets = job.get('job_description') or ''
    company = job.get('company') or ''
    recruiter_name = job.get('recruiter_name') or ''
    profile_snippet = ''
    if profile:
        parts = [profile.get('profile_name') or '', profile.get('profile_subtitle') or '', profile.get('profile_location') or '']
        profile_snippet = ' | '.join([p for p in parts if p])
    return (
        f"Job title: {job.get('job_title','')}\n"
        f"Company: {company}\n"
        f"Hiring manager: {recruiter_name}\n"
        f"Hiring manager profile: {profile_snippet}\n\n"
        f"Key details from job description:\n{bullets}\n"
    )


def build_system_prompt_cv(cv_text: str) -> str:
    return prompts.TAILORED_CV_SYSTEM_PROMPT.format(cv_text=cv_text)


def build_user_prompt_cv(job: dict) -> str:
    bullets = job.get('job_description') or ''
    return (
        f"Job title: {job.get('job_title','')}\n"
        f"Company: {job.get('company','')}\n\n"
        f"Key details from job description (for tailoring):\n{bullets}\n"
    )


def open_csv_writer_for_today():
    ensure_dirs()
    # Include timestamp to avoid collisions and mirror search output naming
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = os.path.join(config.OUTREACH_OUTPUT_DIR, f"outreach_{ts}.csv")
    new_file = not os.path.exists(csv_path)
    f = open(csv_path, 'a', encoding='utf-8', newline='')
    fieldnames = config.OUTREACH_CSV_COLUMNS + ['tailored cv','message']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if new_file:
        writer.writeheader()
    return csv_path, f, writer


def main():
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    processed = load_processed_ids()

    # Build cache index from prior search runs: job_id -> last run_ts
    cache_index = {}
    try:
        if os.path.exists(config.PROCESSED_IDS_PATH):
            with open(config.PROCESSED_IDS_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                # Iterate run_ts sorted ascending so last write wins (latest run)
                for run_ts in sorted(data.keys()):
                    ids = data.get(run_ts) or []
                    if isinstance(ids, list):
                        for jid in ids:
                            cache_index[str(jid)] = run_ts
    except Exception as e:
        print(f"[CACHE] ERROR building cache index: {e}")
        print(traceback.format_exc())

    # Normalize input to a list of URLs
    urls = CONFIG['job_url']
    if isinstance(urls, str):
        # Support multiline strings: split into lines and strip empties
        split_lines = [u.strip() for u in urls.splitlines()]
        urls = [u for u in split_lines if u and (u.startswith('http://') or u.startswith('https://'))]

    # Fallback: if no URLs, use today's search output with fit>3
    if not urls:
        try:
            today_prefix = datetime.datetime.now().strftime('%Y%m%d')
            dir_path = config.OUTREACH_OUTPUT_DIR
            candidates = []
            for name in os.listdir(dir_path):
                if name.startswith(f"search_{today_prefix}") and name.endswith('.csv'):
                    candidates.append(os.path.join(dir_path, name))
            urls = []
            for csv_file in candidates:
                try:
                    with open(csv_file, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            fit_raw = str(row.get('fit') or '')
                            # extract integer from string, default 0
                            digits = ''.join(ch for ch in fit_raw if ch.isdigit())
                            fit_val = int(digits) if digits else 0
                            if fit_val > 3:
                                u = (row.get('job url') or '').strip()
                                if u:
                                    urls.append(u)
                except Exception:
                    pass
            # Deduplicate preserving order
            seen = set()
            deduped = []
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    deduped.append(u)
            urls = deduped
            print(f"[FALLBACK] Selected {len(urls)} URLs from today's search CSVs with fit>3")
        except Exception as e:
            print(f"[FALLBACK] ERROR collecting today's URLs: {e}")
            print(traceback.format_exc())

    cv_text = read_cv_text(CONFIG['cv_file'])
    csv_path, fh, writer = open_csv_writer_for_today()

    # Normalize and pre-extract ids; drop invalid and already processed
    url_items = []
    for url in urls:
        try:
            jid = extract_job_id(url)
        except Exception as e:
            print(f"[SKIP] extract id failed for url='{url}': {e}")
            continue
        if jid in processed:
            print(f"[SKIP] outreach already processed id={jid}")
            continue
        url_items.append((url, jid))

    if not url_items:
        print("[BATCH] No new URLs to process")
        fh.close()
        return

    total = len(url_items)
    batch_size = max(1, CONFIG.get('batch_size', 5))
    batches = [url_items[i:i+batch_size] for i in range(0, total, batch_size)]
    total_batches = len(batches)

    print(f"[BATCH] total_urls={total} batch_size={batch_size} total_batches={total_batches}")

    def process_item(item):
        url, job_id = item
        try:
            # Try cache reuse from prior search CSV
            job_details = None
            recruiter_link = ''
            recruiter_name = ''
            cached_run = cache_index.get(str(job_id))
            if cached_run:
                try:
                    search_csv = os.path.join(config.OUTREACH_OUTPUT_DIR, f"search_{cached_run}.csv")
                    if os.path.exists(search_csv):
                        with open(search_csv, 'r', encoding='utf-8') as cf:
                            reader = csv.DictReader(cf)
                            for row in reader:
                                try:
                                    rid = extract_job_id(row.get('job url') or '')
                                except Exception:
                                    continue
                                if rid == job_id:
                                    job_details = {
                                        'job_title': row.get('job title') or '',
                                        'job_description': row.get('description') or '',
                                        'company': row.get('company name') or '',
                                        'company_link': row.get('company linkedin url') or '',
                                        'job_link': row.get('job url') or '',
                                        'publishing_date': row.get('upload date') or '',
                                        'posted_time_ago': row.get('upload date') or '',
                                        'recruiter_name': row.get('hiring manager name') or '',
                                        'recruiter_link': row.get('hiring manager linkedin url') or '',
                                    }
                                    recruiter_link = job_details.get('recruiter_link') or ''
                                    recruiter_name = job_details.get('recruiter_name') or ''
                                    print(f"[CACHE] hit id={job_id} run={cached_run}")
                                    break
                except Exception as e:
                    print(f"[CACHE] ERROR reading search CSV for run={cached_run}: {e}")
                    print(traceback.format_exc())

            # Fallback to fresh scrape when cache miss
            if not job_details:
                try:
                    job_details, _ = fetch_job_details(job_id)
                except Exception as e:
                    print(f"ERROR fetching job details for {job_id}: {e}")
                    print(traceback.format_exc())
                    return None
                if not job_details:
                    print(f"ERROR: No job details for {job_id}")
                    return None
                recruiter_link = job_details.get('recruiter_link') or ''
                recruiter_name = job_details.get('recruiter_name') or ''

            profile = None
            if recruiter_link:
                try:
                    profile = fetch_public_profile(recruiter_link)
                except Exception as e:
                    print(f"ERROR fetching recruiter profile: {e}")
                    print(traceback.format_exc())

            fit_val = ''
            message = ''
            tailored_cv = ''
            try:
                if recruiter_link:
                    sys_prompt = build_system_prompt_outreach(cv_text)
                    usr_prompt = build_user_prompt_outreach(job_details, profile or {})
                    content, _, _ = call_llm(sys_prompt, usr_prompt, response_format={"type": "json_object"})
                    if isinstance(content, dict):
                        fit_val = str(content.get('fit', ''))
                        message = content.get('message', '') or ''
                    else:
                        import json as _json
                        try:
                            parsed = _json.loads(content or '{}')
                            fit_raw = parsed.get('fit')
                            fit_val = str(fit_raw) if fit_raw is not None else ''
                            message = parsed.get('message', '') or ''
                        except Exception:
                            pass
                else:
                    sys_prompt = build_system_prompt_cv(cv_text)
                    usr_prompt = build_user_prompt_cv(job_details)
                    content, _, _ = call_llm(sys_prompt, usr_prompt, response_format={"type": "json_object"})
                    if isinstance(content, dict):
                        tailored_cv = content.get('tailored_cv', '') or ''
                    else:
                        import json as _json
                        try:
                            parsed = _json.loads(content or '{}')
                            tailored_cv = parsed.get('tailored_cv', '') or ''
                        except Exception:
                            pass
            except Exception as e:
                print(f"ERROR LLM: {e}")
                print(traceback.format_exc())

            row = {
                'id': job_id,
                'job title': job_details.get('job_title') or '',
                'description': job_details.get('job_description') or '',
                'company name': job_details.get('company') or '',
                'company linkedin url': job_details.get('company_link') or '',
                'job url': job_details.get('job_link') or '',
                'upload date': job_details.get('publishing_date') or job_details.get('posted_time_ago') or '',
                'hiring manager name': recruiter_name,
                'hiring manager linkedin url': recruiter_link or '',
                'fit': fit_val,
                'message': message,
                'tailored cv': tailored_cv,
            }
            return row, job_id
        except Exception as e:
            print(f"[BATCH] ERROR item id={job_id}: {e}")
            print(traceback.format_exc())
            return None

    def process_batch(batch_idx: int, items: list):
        batch_rows = []
        batch_ids = set()
        print(f"[BATCH] start {batch_idx+1}/{total_batches} size={len(items)}")
        real_workers = max(1, min(CONFIG.get('max_workers', 5), len(items), batch_size))
        with ThreadPoolExecutor(max_workers=real_workers) as ex:
            futures = [ex.submit(process_item, item) for item in items]
            for fut in as_completed(futures):
                result = fut.result()
                if not result:
                    continue
                row, jid = result
                batch_rows.append(row)
                batch_ids.add(jid)
        # Sort for deterministic order
        batch_rows.sort(key=lambda r: (r.get('company name') or '').lower())
        return batch_rows, batch_ids
        batch_rows = []
        batch_ids = set()
        print(f"[BATCH] start {batch_idx+1}/{total_batches} size={len(items)}")
        for url, job_id in items:
            try:
                # Try cache reuse from prior search CSV
                job_details = None
                recruiter_link = ''
                recruiter_name = ''
                cached_run = cache_index.get(str(job_id))
                if cached_run:
                    try:
                        search_csv = os.path.join(config.OUTREACH_OUTPUT_DIR, f"search_{cached_run}.csv")
                        if os.path.exists(search_csv):
                            with open(search_csv, 'r', encoding='utf-8') as cf:
                                reader = csv.DictReader(cf)
                                for row in reader:
                                    try:
                                        rid = extract_job_id(row.get('job url') or '')
                                    except Exception:
                                        continue
                                    if rid == job_id:
                                        job_details = {
                                            'job_title': row.get('job title') or '',
                                            'job_description': row.get('description') or '',
                                            'company': row.get('company name') or '',
                                            'company_link': row.get('company linkedin url') or '',
                                            'job_link': row.get('job url') or '',
                                            'publishing_date': row.get('upload date') or '',
                                            'posted_time_ago': row.get('upload date') or '',
                                            'recruiter_name': row.get('hiring manager name') or '',
                                            'recruiter_link': row.get('hiring manager linkedin url') or '',
                                        }
                                        recruiter_link = job_details.get('recruiter_link') or ''
                                        recruiter_name = job_details.get('recruiter_name') or ''
                                        print(f"[CACHE] hit id={job_id} run={cached_run}")
                                        break
                    except Exception as e:
                        print(f"[CACHE] ERROR reading search CSV for run={cached_run}: {e}")
                        print(traceback.format_exc())

                # Fallback to fresh scrape when cache miss
                if not job_details:
                    try:
                        job_details, _ = fetch_job_details(job_id)
                    except Exception as e:
                        print(f"ERROR fetching job details for {job_id}: {e}")
                        print(traceback.format_exc())
                        continue
                    if not job_details:
                        print(f"ERROR: No job details for {job_id}")
                        continue
                    recruiter_link = job_details.get('recruiter_link') or ''
                    recruiter_name = job_details.get('recruiter_name') or ''

                profile = None
                if recruiter_link:
                    try:
                        profile = fetch_public_profile(recruiter_link)
                    except Exception as e:
                        print(f"ERROR fetching recruiter profile: {e}")
                        print(traceback.format_exc())

                fit_val = ''
                message = ''
                tailored_cv = ''
                try:
                    if recruiter_link:
                        sys_prompt = build_system_prompt_outreach(cv_text)
                        usr_prompt = build_user_prompt_outreach(job_details, profile or {})
                        content, _, _ = call_llm(sys_prompt, usr_prompt, response_format={"type": "json_object"})
                        if isinstance(content, dict):
                            fit_val = str(content.get('fit', ''))
                            message = content.get('message', '') or ''
                        else:
                            import json as _json
                            try:
                                parsed = _json.loads(content or '{}')
                                fit_raw = parsed.get('fit')
                                fit_val = str(fit_raw) if fit_raw is not None else ''
                                message = parsed.get('message', '') or ''
                            except Exception:
                                pass
                    else:
                        sys_prompt = build_system_prompt_cv(cv_text)
                        usr_prompt = build_user_prompt_cv(job_details)
                        content, _, _ = call_llm(sys_prompt, usr_prompt, response_format={"type": "json_object"})
                        if isinstance(content, dict):
                            tailored_cv = content.get('tailored_cv', '') or ''
                        else:
                            import json as _json
                            try:
                                parsed = _json.loads(content or '{}')
                                tailored_cv = parsed.get('tailored_cv', '') or ''
                            except Exception:
                                pass
                except Exception as e:
                    print(f"ERROR LLM: {e}")
                    print(traceback.format_exc())

                batch_rows.append({
                    'job title': job_details.get('job_title') or '',
                    'description': job_details.get('job_description') or '',
                    'company name': job_details.get('company') or '',
                    'company linkedin url': job_details.get('company_link') or '',
                    'job url': job_details.get('job_link') or '',
                    'upload date': job_details.get('publishing_date') or job_details.get('posted_time_ago') or '',
                    'hiring manager name': recruiter_name,
                    'hiring manager linkedin url': recruiter_link or '',
                    'fit': fit_val,
                    'message': message,
                    'tailored cv': tailored_cv,
                })
                batch_ids.add(job_id)
            except Exception as e:
                print(f"[BATCH] ERROR item id={job_id}: {e}")
                print(traceback.format_exc())
        # Sort for deterministic order
        batch_rows.sort(key=lambda r: (r.get('company name') or '').lower())
        return batch_rows, batch_ids

    written = 0
    new_ids = set()
    try:
        for i, items in enumerate(batches):
            try:
                rows, ids = process_batch(i, items)
            except Exception as e:
                print(f"[BATCH] ERROR batch {i+1}: {e}")
                print(traceback.format_exc())
                continue
            try:
                for r in rows:
                    writer.writerow(r)
                fh.flush()
                os.fsync(fh.fileno())
                written += len(rows)
                new_ids.update(ids)
                # Persist processed ids incrementally
                tmp_ids = set(processed)
                tmp_ids.update(new_ids)
                append_run_processed_ids(ts, tmp_ids)
                print(f"[WRITE] batch {i+1}/{total_batches} wrote={len(rows)} total={written}")
            except Exception as e:
                print(f"[WRITE] ERROR writing batch {i+1}: {e}")
                print(traceback.format_exc())
    finally:
        fh.close()

    processed.update(new_ids)
    append_run_processed_ids(ts, processed)
    print(f"Wrote {written} row(s) to {csv_path}")


if __name__ == '__main__':
    main()


