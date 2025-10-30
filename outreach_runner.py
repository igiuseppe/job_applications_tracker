import csv
import datetime
import json
import os
import random
import time
from typing import List

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
    'contract_types': ['Contract'],
    # Path to a txt/md file with your CV content
    'cv_file': 'cv.txt',
    # Time posted filter: one of {'Any','Past 24 hours','Past Week','Past Month'}
    'time_posted': 'Past Week',
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
        except Exception:
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
    return (
        "You are an expert LinkedIn outreach assistant. "
        "Write concise, polite, high-signal messages (4-6 lines), no emojis. "
        "Personalize with specifics from the job and hiring manager profile.\n\n"
        "Candidate CV (verbatim):\n" + cv_text
    )


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
        f"Key details from job description:\n{bullets}"
    )


def write_csv(rows: List[dict], timestamp_str: str):
    ensure_dirs()
    csv_path = os.path.join(config.OUTREACH_OUTPUT_DIR, f"outreach_{timestamp_str}.csv")
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=config.OUTREACH_CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return csv_path


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

    rows = []
    new_ids = set()

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

                for job in jobs or []:
                    jid = job.get('id')
                    if not jid or jid in processed_ids or jid in new_ids:
                        continue

                    recruiter_link = job.get('recruiter_link')
                    profile = None
                    if recruiter_link:
                        profile = fetch_public_profile(recruiter_link)
                        time.sleep(random.uniform(0.3, 0.8))

                    # Build LLM prompts
                    user_prompt = build_user_prompt(job, profile or {}, country, work_type_name, contract_input)
                    try:
                        message, _, _ = call_llm_litellm(system_prompt, user_prompt)
                    except Exception:
                        message = ""

                    rows.append({
                        'job title': job.get('job_title') or '',
                        'description': job.get('job_description') or '',
                        'company name': job.get('company') or '',
                        'company linkedin url': job.get('company_link') or '',
                        'job url': job.get('job_link') or '',
                        'hiring manager name': job.get('recruiter_name') or '',
                        'hiring manager linkedin url': recruiter_link or '',
                        'message': message or '',
                    })

                    new_ids.add(jid)

    # write CSV
    csv_path = write_csv(rows, timestamp_str)

    # persist ids
    processed_ids.update(new_ids)
    save_processed_ids(processed_ids)

    print(f"Wrote {len(rows)} rows to {csv_path}")


if __name__ == '__main__':
    main()


