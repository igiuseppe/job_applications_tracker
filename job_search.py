import csv
import datetime
import json
import os
import random
import time
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

import config
import prompts
from linkedin_scraper import scrape_linkedin_jobs, fetch_public_profile
from utils import call_llm

# In-script configuration (no CLI)
CONFIG = {
    # Comma-separated keywords as list
    'keywords': [
        'AI Engineer', 'Data Engineer'
    ],
    # Countries must match keys in scrape_jobs.GEO_IDS
    'countries': [
        'Germany', 'Switzerland','France', 'Italy'
    ],
    # Pages per keyword×country×work_type
    'pages': 3,
    # Subset of {Remote, Hybrid, On-site}
    'work_types': ['Remote'],
    # Subset of {Full-time, Contract, Part-time, Temporary, Internship, Other}
    'contract_types': ['Full-time', 'Contract', 'Part-time', 'Temporary'],
    # Path to a txt/md file with your CV content
    'cv_file': 'cv.txt',
    # Time posted filter: one of {'Any','Past 24 hours','Past Week','Past Month'}
    'time_posted': 'Past 24 hours',
    # Batch size for processing jobs (failures isolate to a single batch)
    'batch_size': 10,
    # Max parallel LLM calls
    'max_workers': 5,
}


def ensure_dirs():
    os.makedirs(config.OUTREACH_OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.STATE_DIR, exist_ok=True)


def load_processed_ids() -> set:
    ensure_dirs()
    if os.path.exists(config.PROCESSED_IDS_PATH):
        try:
            with open(config.PROCESSED_IDS_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Support legacy list format and new dict format {run_ts: [ids]}
            if isinstance(data, list):
                return set(data)
            if isinstance(data, dict):
                all_ids = set()
                for _, ids in data.items():
                    if isinstance(ids, list):
                        all_ids.update(ids)
                return all_ids
            return set()
        except Exception as e:
            print(f"ERROR load_processed_ids: {e}")
            print(traceback.format_exc())
            return set()
    return set()


def append_run_processed_ids(run_timestamp: str, ids: set):
    """Persist processed IDs grouped by run timestamp. Overwrites the entry for this run timestamp."""
    ensure_dirs()
    existing = {}
    if os.path.exists(config.PROCESSED_IDS_PATH):
        try:
            with open(config.PROCESSED_IDS_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                existing = data
            elif isinstance(data, list):
                # migrate legacy list into a synthetic key
                existing = {"legacy": data}
        except Exception as e:
            print(f"ERROR reading existing processed ids: {e}")
            print(traceback.format_exc())
            existing = {}
    existing[run_timestamp] = sorted(list(ids))
    with open(config.PROCESSED_IDS_PATH, 'w', encoding='utf-8') as f:
        json.dump(existing, f)


def encode_keywords(keyword: str) -> str:
    # Minimal encoding for LinkedIn query: spaces -> +, then + -> %2B
    return keyword.strip().replace(' ', '+').replace('+', '%2B')


def map_contract_types(input_values: List[str]) -> List[str]:
    codes = []
    for v in input_values:
        v_clean = v.strip()
        code = config.CONTRACT_TYPE_CODES.get(v_clean)
        if code:
            codes.append(code)
    return codes


def map_time_posted(label: str) -> str:
    return config.TIME_POSTED_CODES.get(label.strip(), "")


def read_cv_text(cv_path: str) -> str:
    with open(cv_path, 'r', encoding='utf-8') as f:
        return f.read()


def build_system_prompt(cv_text: str) -> str:
    # Fit-only system prompt
    return prompts.FIT_SYSTEM_PROMPT.format(cv_text=cv_text)


def build_user_prompt(job: dict, profile: dict, country: str, work_type_name: str, contract_types: List[str]) -> str:
    bullets = job.get('job_description', '') or ''
    company = job.get('company') or ''
    recruiter_name = job.get('recruiter_name') or ''
    profile_snippet = ''
    if profile:
        parts = [
            profile.get('profile_name') or '',
            profile.get('profile_subtitle') or '',
            profile.get('profile_location') or '',
        ]
        profile_snippet = ' | '.join([p for p in parts if p])

    ct = ','.join(contract_types) if contract_types else ''
    return (
        f"Job title: {job.get('job_title','')}\n"
        f"Company: {company}\n"
        f"Country: {country}\n"
        f"Hiring manager name: {recruiter_name}\n"
        f"Hiring manager profile: {profile_snippet}\n\n"
        f"Key details from job description:\n{bullets}\n\n"
    )


def parse_fit_and_message(content: str):
    fit_val = ''
    message = content or ''
    try:
        lines = (content or '').splitlines()
        # Find line starting with FIT:
        for i, line in enumerate(lines):
            if line.strip().upper().startswith('FIT:'):
                num = line.split(':', 1)[1].strip()
                # keep only digits and clamp to 1-10
                num_digits = ''.join(ch for ch in num if ch.isdigit())
                if num_digits:
                    n = max(1, min(10, int(num_digits)))
                    fit_val = str(n)
                # message section is everything after a line starting with MESSAGE:
                msg_start = None
                for j in range(i + 1, len(lines)):
                    if lines[j].strip().upper().startswith('MESSAGE'):
                        msg_start = j + 1
                        break
                if msg_start is not None:
                    message = '\n'.join(lines[msg_start:]).strip()
                break
    except Exception as e:
        print(f"ERROR parse_fit_and_message: {e}")
        print(traceback.format_exc())
    return fit_val, message


def open_csv_writer(timestamp_str: str):
    ensure_dirs()
    # Decoupled name for job search outputs to avoid overlap with outreach
    csv_path = os.path.join(config.OUTREACH_OUTPUT_DIR, f"search_{timestamp_str}.csv")
    f = open(csv_path, 'w', encoding='utf-8', newline='')
    writer = csv.DictWriter(f, fieldnames=config.OUTREACH_CSV_COLUMNS)
    writer.writeheader()
    return csv_path, f, writer


def chunked(items: List[dict], size: int):
    if not items:
        return []
    for i in range(0, len(items), size):
        yield items[i:i + size]


def main():
    timestamp_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    processed_ids = load_processed_ids()

    cv_text = read_cv_text(CONFIG['cv_file'])
    system_prompt = build_system_prompt(cv_text)

    keywords = [k.strip() for k in CONFIG['keywords'] if k.strip()]
    countries = [c.strip() for c in CONFIG['countries'] if c.strip()]
    work_types = [w.strip() for w in CONFIG['work_types'] if w.strip()]
    contract_input = [c.strip() for c in CONFIG['contract_types'] if c.strip()]
    contract_codes = map_contract_types(contract_input)
    time_posted_code = map_time_posted(CONFIG.get('time_posted', 'Any'))

    new_ids = set()
    csv_path, csv_file, csv_writer = open_csv_writer(timestamp_str)
    total_rows = 0

    # High-level grid progress
    total_combos = sum(1 for _ in [(kw, ct, wt) for ct in countries for wt in work_types for kw in keywords])
    combo_idx = 0

    for country in countries:
        if country not in config.GEO_IDS:
            continue
        geo_id = config.GEO_IDS[country]
        for work_type_name, work_type_val in config.WORK_TYPES.items():
            # filter only selected work types
            if work_type_name not in work_types:
                continue
            for kw in keywords:
                combo_idx += 1
                print(f"[GRID] {combo_idx}/{total_combos} → kw='{kw}', country='{country}', work_type='{work_type_name}', pages={CONFIG['pages']}")
                encoded_kw = encode_keywords(kw)
                # scrape
                jobs = scrape_linkedin_jobs(
                    keywords=encoded_kw,
                    location=country,
                    geoId=geo_id,
                    work_type=work_type_val,
                    jobs_per_page=10,
                    max_pages=CONFIG['pages'],
                    search_keyword_job_title=kw,
                    contract_types=contract_codes if contract_codes else None,
                    time_posted_code=time_posted_code,
                )
                time.sleep(random.uniform(0.5, 1.0))
                print(f"[SCRAPE] Found {len(jobs or [])} jobs for kw='{kw}', country='{country}', work_type='{work_type_name}'")

                # Process in batches; if a batch fails, skip only that batch
                for batch in chunked(jobs or [], CONFIG.get('batch_size', 10)):
                    total_batches = max(1, (len(jobs or []) + CONFIG.get('batch_size', 10) - 1) // CONFIG.get('batch_size', 10))
                    print(f"[BATCH] Start batch ({len(batch)} items) for kw='{kw}', country='{country}', work_type='{work_type_name}' [{combo_idx}/{total_combos}] -> size={CONFIG.get('batch_size',10)} total_batches={total_batches}")
                    try:
                        batch_rows = []
                        batch_new_ids = set()
                        llm_futures = {}
                        max_workers = max(1, min(CONFIG.get('batch_size', 10), CONFIG.get('max_workers', 8)))
                        executor = ThreadPoolExecutor(max_workers=max_workers)
                        for job in batch:
                            jid = job.get('id')
                            if not jid or jid in processed_ids or jid in new_ids:
                                continue

                            recruiter_link = job.get('recruiter_link')
                            profile = None
                            if recruiter_link:
                                profile = fetch_public_profile(recruiter_link)
                                time.sleep(random.uniform(0.3, 0.8))

                            # Prepare row; LLM to be filled later (parallel)
                            row = {
                                'job title': job.get('job_title') or '',
                                'description': job.get('job_description') or '',
                                'company name': job.get('company') or '',
                                'company linkedin url': job.get('company_link') or '',
                                'job url': job.get('job_link') or '',
                                'upload date': job.get('publishing_date') or job.get('posted_time_ago') or '',
                                'hiring manager name': job.get('recruiter_name') or '',
                                'hiring manager linkedin url': recruiter_link or '',
                                'fit': '',
                                'message': '',
                            }
                            batch_rows.append(row)
                            batch_new_ids.add(jid)

                            if recruiter_link:
                                user_prompt = build_user_prompt(job, profile or {}, country, work_type_name, contract_input)
                                def _llm_call(sp=system_prompt, up=user_prompt):
                                    try:
                                        content, _, _ = call_llm(
                                            sp,
                                            up,
                                            response_format={"type": "json_object"},
                                            model="gemini/gemini-2.5-flash",
                                        )
                                        return content
                                    except Exception as e:
                                        print(f"ERROR LLM call: {e}")
                                        print(traceback.format_exc())
                                        return ""
                                future = executor.submit(_llm_call)
                                llm_futures[future] = row

                        # Collect LLM results
                        for fut in as_completed(llm_futures):
                            row = llm_futures[fut]
                            content = fut.result()
                            if isinstance(content, dict):
                                row['fit'] = str(content.get('fit', ''))
                                # message intentionally left empty in search phase
                            else:
                                try:
                                    import json as _json
                                    parsed = _json.loads(content or '{}')
                                    fit_raw = parsed.get('fit')
                                    row['fit'] = str(fit_raw) if fit_raw is not None else ''
                                    # message intentionally left empty in search phase
                                except Exception:
                                    f, m = parse_fit_and_message(content)
                                    row['fit'] = f
                        executor.shutdown(wait=True)
                    except Exception as e:
                        print(f"ERROR processing batch of size {len(batch)}: {e}")
                        print(traceback.format_exc())
                        # skip this batch and continue
                        continue

                    # If batch succeeded, persist rows and ids now
                    try:
                        for r in batch_rows:
                            csv_writer.writerow(r)
                        csv_file.flush()
                        os.fsync(csv_file.fileno())
                        total_rows += len(batch_rows)
                        new_ids.update(batch_new_ids)
                        # Persist processed ids incrementally
                        tmp_ids = set(processed_ids)
                        tmp_ids.update(new_ids)
                        append_run_processed_ids(timestamp_str, tmp_ids)
                        print(f"[BATCH] Wrote {len(batch_rows)} rows | cumulative_rows={total_rows}")
                    except Exception as e:
                        print(f"ERROR writing batch to CSV: {e}")
                        print(traceback.format_exc())

    # close CSV and persist ids at end
    csv_file.close()
    processed_ids.update(new_ids)
    append_run_processed_ids(timestamp_str, processed_ids)

    print(f"Wrote {total_rows} rows to {csv_path}")


if __name__ == '__main__':
    main()


