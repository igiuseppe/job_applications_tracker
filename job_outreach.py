import re
import os
import csv
import json
import datetime
import traceback

import config
from linkedin_scraper import fetch_job_details, fetch_public_profile
from utils import call_llm
import prompts

job="https://www.linkedin.com/jobs/view/4331434596/?refId=hhJRhqL4Q4KWjVQpWpGI7g%3D%3D&trackingId=hhJRhqL4Q4KWjVQpWpGI7g%3D%3D"


CONFIG = {
    'job_url': job,  # Can be a single URL (str) or a list of URLs
    'cv_file': 'cv.txt',
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
    if os.path.exists(config.PROCESSED_IDS_PATH):
        try:
            with open(config.PROCESSED_IDS_PATH, 'r', encoding='utf-8') as f:
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
    with open(config.PROCESSED_IDS_PATH, 'w', encoding='utf-8') as f:
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
    today = datetime.datetime.now().strftime('%Y%m%d')
    csv_path = os.path.join(config.OUTREACH_OUTPUT_DIR, f"outreach_{today}.csv")
    new_file = not os.path.exists(csv_path)
    f = open(csv_path, 'a', encoding='utf-8', newline='')
    fieldnames = config.OUTREACH_CSV_COLUMNS + ['tailored cv']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    if new_file:
        writer.writeheader()
    return csv_path, f, writer


def main():
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    processed = load_processed_ids()

    # Normalize input to a list of URLs
    urls = CONFIG['job_url']
    if isinstance(urls, str):
        urls = [urls]

    cv_text = read_cv_text(CONFIG['cv_file'])
    csv_path, fh, writer = open_csv_writer_for_today()

    written = 0
    try:
        for url in urls:
            try:
                job_id = extract_job_id(url)
            except Exception as e:
                print(f"ERROR extracting job id from url '{url}': {e}")
                print(traceback.format_exc())
                continue

            if job_id in processed:
                print(f"ERROR: job id {job_id} already processed. Skipping.")
                continue

            try:
                job_details, _ = fetch_job_details(job_id)
            except Exception as e:
                print(f"ERROR fetching job details for {job_id}: {e}")
                print(traceback.format_exc())
                continue

            if not job_details:
                print(f"ERROR: No job details for {job_id}")
                continue

            recruiter_link = job_details.get('recruiter_link')
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

            writer.writerow({
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
            fh.flush()
            os.fsync(fh.fileno())

            processed.add(job_id)
            written += 1
    finally:
        fh.close()

    # Persist processed ids under this run timestamp once
    append_run_processed_ids(ts, processed)
    print(f"Wrote {written} row(s) to {csv_path}")


if __name__ == '__main__':
    main()


