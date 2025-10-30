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
from linkedin_scraper import scrape_linkedin_jobs, fetch_public_profile
from scrape_jobs import GEO_IDS, WORK_TYPES
from utils import call_llm_litellm, truncate

# In-script configuration (no CLI)
CONFIG = {
    # Comma-separated keywords as list
    'keywords': [
        'AI Engineer',
    ],
    # Countries must match keys in scrape_jobs.GEO_IDS
    'countries': [
        'Germany'
    ],
    # Pages per keyword×country×work_type
    'pages': 1,
    # Subset of {Remote, Hybrid, On-site}
    'work_types': ['Remote'],
    # Subset of {Full-time, Contract, Part-time, Temporary, Internship, Other}
    'contract_types': ['Full-time'],
    # Path to a txt/md file with your CV content
    'cv_file': 'cv.txt',
    # Time posted filter: one of {'Any','Past 24 hours','Past Week','Past Month'}
    'time_posted': 'Past Week',
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
            return set(data if isinstance(data, list) else [])
        except Exception as e:
            print(f"ERROR load_processed_ids: {e}")
            print(traceback.format_exc())
            return set()
    return set()


def save_processed_ids(ids: set):
    ensure_dirs()
    with open(config.PROCESSED_IDS_PATH, 'w', encoding='utf-8') as f:
        json.dump(sorted(list(ids)), f)


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
    prompt = (
        "You are an expert freelance job outreach coach focused on LinkedIn. "
        "Goal: maximize replies and book a short call. "
        "Constraints: concise, specific, polite, no emojis, 4–6 lines. "
        "Personalize using both the job description and hiring manager profile (role, domain, stack, location). "
        "Avoid generic fluff; reference 1–2 concrete matches to my background. "
        "If the job clearly mismatches my background, lower the fit score and suggest one pragmatic angle.\n\n"
        "My CV (verbatim):\n" + cv_text + "\n\n"
        "Output strictly a minified JSON object with two keys: "
        "{\"fit\": <integer 1-10>, \"message\": \"<4-6 line tailored message>\"}. "
        "Do not include any other text."
    )
    return prompt


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
    csv_path = os.path.join(config.OUTREACH_OUTPUT_DIR, f"outreach_{timestamp_str}.csv")
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

    for country in countries:
        if country not in GEO_IDS:
            continue
        geo_id = GEO_IDS[country]
        for work_type_name, work_type_val in WORK_TYPES.items():
            # filter only selected work types
            if work_type_name not in work_types:
                continue
            for kw in keywords:
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

                # Process in batches; if a batch fails, skip only that batch
                for batch in chunked(jobs or [], CONFIG.get('batch_size', 10)):
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
                                        content, _, _ = call_llm_litellm(
                                            sp,
                                            up,
                                            response_format={"type": "json_object"},
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
                                row['message'] = content.get('message', '') or ''
                            else:
                                try:
                                    import json as _json
                                    parsed = _json.loads(content or '{}')
                                    fit_raw = parsed.get('fit')
                                    row['fit'] = str(fit_raw) if fit_raw is not None else ''
                                    row['message'] = parsed.get('message', '') or ''
                                except Exception:
                                    f, m = parse_fit_and_message(content)
                                    row['fit'], row['message'] = f, m
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
                        save_processed_ids(tmp_ids)
                    except Exception as e:
                        print(f"ERROR writing batch to CSV: {e}")
                        print(traceback.format_exc())

    # close CSV and persist ids at end
    csv_file.close()
    processed_ids.update(new_ids)
    save_processed_ids(processed_ids)

    print(f"Wrote {total_rows} rows to {csv_path}")


if __name__ == '__main__':
    main()


