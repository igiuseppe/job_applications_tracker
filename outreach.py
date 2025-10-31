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

job="""
https://de.linkedin.com/jobs/view/ai-engineer-at-pitch-4332989238?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/ml-engineer-fully-remote-at-mercor-4320047628?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/machine-learning-engineer-at-mercor-4320007615?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/ai-engineer-m-w-d-at-neue-osnabr%C3%BCcker-zeitung-gmbh-co-kg-4320066609?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/ai-software-engineer-ab-januar-2026-teilzeit-remote-und-m%C3%BCnster-m-w-d-at-emagine-4319362730?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/ai-engineer-team-lead-at-pitch-4333118758?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/machine-learning-engineer-remote-at-mercor-4319934863?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/machine-learning-engineer-ai-systems-at-mercor-4319857894?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/data-science-and-machine-learning-engineer-m-f-d-at-gaia-4332966867?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/deep-learning-engineer-at-mercor-4319977774?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/analytics-engineer-at-codex-4333106180?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/data-engineer-m-f-d-at-carl-remigius-fresenius-education-group-4333294527?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/business-intelligence-analyst-at-codex-4333036387?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/junior-devops-engineer-at-eurobase-people-4333422697?trk=public_jobs_topcard-title
https://de.linkedin.com/jobs/view/sql-database-architect-at-net2source-n2s-4312037994?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/senior-analytics-engineer-at-seeq-corporation-4333177346?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/software-engineer-80%25-100%25-bern-at-code-compass-%F0%9F%A7%AD-4319878206?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/agentic-and-openai-automation-expert-at-qi-talance-4333210620?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/business-analyst-at-digify-experts-4319878770?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/data-visualization-specialist-at-at8-media-4319978752?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/customer-insights-analyst-at-eden-development-4320090797?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/rust-engineer-80%25-100%25-%E2%80%93-zurich-at-code-compass-%F0%9F%A7%AD-4319968119?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/backend-engineer-at-calyptus-4333274315?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/data-architect-at-infinity-quest-4319273790?trk=public_jobs_topcard-title
https://ch.linkedin.com/jobs/view/data-architect-aws-redshift-at-eures-limited-4319878741?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/ai-machine-learning-r-d-engineer-at-welocalize-4333411973?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/ing%C3%A9nieur-ia-h-f-nb-at-silae-4331130570?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/senior-machine-learning-engineer-at-photoroom-4333109684?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/machine-learning-engineer-relocation-to-spain-required-at-brainrocket-4331445955?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/data-scientist-at-datadome-4307674764?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/data-engineer-at-digital-forge-4319878535?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/freelancer-full-stack-django-react-developer-remote-europe-at-guestready-4319912109?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/senior-backend-engineer-kotlin-fincrime-at-mlabs-4333108408?trk=public_jobs_topcard-title
https://fr.linkedin.com/jobs/view/backend-engineer-at-calyptus-4333382942?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/junior-ai-engineer-remote-europe-at-jobgether-4319887489?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/artificial-intelligence-engineer-at-mashfrog-group-4318904342?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/machine-learning-engineer-relocation-to-spain-required-at-brainrocket-4331457193?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/data-analyst-0-experience-required-at-peroptyx-4332497564?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/junior-ai-engineer-for-martech-at-joinrs-4333304599?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/ai-software-engineer-italy-at-ekona-ai-4319924868?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/senior-backend-engineer-python-italy-remote-at-plentific-4320066993?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/data-engineer-at-amaris-consulting-4331702126?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/data-engineer-at-ul-solutions-4278803239?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/senior-data-engineer-at-agap2-italia-4331434596?trk=public_jobs_topcard-title
https://it.linkedin.com/jobs/view/platform-developer-analyst-at-equis-founding-team-at-founders-factory-4333220924?trk=public_jobs_topcard-title
"""

CONFIG = {
    'job_url': job,  # Can be a single URL (str) or a list of URLs
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


