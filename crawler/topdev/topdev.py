import csv
import json
import os
import re
import shutil
import subprocess
import time
import random
import threading
from datetime import datetime
from html import unescape as html_unescape
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# =====================================
# Configuration
# =====================================
BASE = "https://topdev.vn"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

MAX_WORKERS = 5
AUTOSAVE_PATH = os.path.join(SCRIPT_DIR, "topdev_autosave.csv")
FINAL_CSV = os.path.join(SCRIPT_DIR, "topdev_jobs.csv")
AUTOSAVE_INTERVAL = 10
DRIVER_LOCK = threading.RLock()
LISTING_URL = "https://topdev.vn/jobs/search"
MAX_PAGES = 70

_thread_local = threading.local()
_all_drivers: List[webdriver.Chrome] = []

# Skills list for extraction from text
KNOWN_SKILLS = [
    "python", "java", "c#", "c++", "javascript", "typescript",
    "react", "angular", "vue", "nodejs", "nestjs",
    "docker", "kubernetes", "aws", "azure", "gcp",
    "sql", "mysql", "postgresql", "mongodb", "redis",
    "kafka", "spark", "airflow", "tensorflow", "pytorch",
    "fastapi", "flask", "django", "git", "linux", "ai",
    "machine learning", "deep learning", "nlp", "llm",
    "langchain", "llamaindex", "computer vision",
    "oracle", "elasticsearch", "nginx", "spring",
    "golang", "rust", "php", "ruby", "swift", "kotlin",
    "hadoop", "hive", "tableau", "powerbi", "excel",
    "figma", "photoshop", "jira", "agile", "scrum",
    "selenium", "jenkins", "terraform", "ansible"
]

_stats = {
    "total_urls": 0,
    "success": 0,
    "errors": 0,
    "start_time": datetime.now()
}

_all_jobs: List[Dict] = []
_seen_urls: set = set()

# Output columns — identical to vnwork.csv + experience & job_level
COLUMNS = [
    "title",
    "company",
    "salary",
    "location",
    "description",
    "skills",
    "experience",
    "job_level",
    "url",
    "crawl_time",
]

# =====================================
# Utilities
# =====================================

def detect_chrome_binary() -> Optional[str]:
    candidates = [
        shutil.which("chrome"),
        shutil.which("chrome.exe"),
        shutil.which("google-chrome"),
        os.path.join(os.environ.get("ProgramFiles", ""), "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(os.environ.get("ProgramFiles(x86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
    ]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None


def create_driver() -> webdriver.Chrome:
    with DRIVER_LOCK:
        chrome_binary = detect_chrome_binary()
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"--user-agent={USER_AGENT}")
        if chrome_binary:
            options.binary_location = chrome_binary
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)
        _all_drivers.append(driver)
        return driver


def get_thread_driver() -> webdriver.Chrome:
    driver = getattr(_thread_local, "driver", None)
    if driver and getattr(driver, "session_id", None):
        return driver
    driver = create_driver()
    _thread_local.driver = driver
    return driver


def quit_all_drivers():
    for d in _all_drivers:
        try:
            d.quit()
        except Exception:
            pass


def clean_text(text: Optional[str]) -> str:
    """Clean HTML to plain text, collapse all whitespace to a single space (single line)."""
    if not text:
        return ""
    # Unescape HTML entities
    text = html_unescape(str(text))
    # Parse HTML tags
    soup = BeautifulSoup(text, "html.parser")
    # Replace block elements with space separator
    for tag in soup.find_all(["li", "p", "br", "div", "h1", "h2", "h3", "h4"]):
        tag.insert_before(" ")
    plain = soup.get_text(" ")
    # Collapse ALL whitespace (including newlines, tabs) into single space
    plain = re.sub(r"\s+", " ", plain).strip()
    return plain


def extract_skills_from_text(text: str) -> str:
    """Extract known skills from free text, return comma-separated lowercase list."""
    if not text:
        return ""
    text_lower = text.lower()
    found = []
    for skill in KNOWN_SKILLS:
        # Use word boundary for single-word skills
        pattern = r'\b' + re.escape(skill) + r'\b'
        if re.search(pattern, text_lower):
            found.append(skill)
    return ", ".join(found)


def parse_jsonld(soup: BeautifulSoup) -> Optional[Dict]:
    """Extract first JobPosting from JSON-LD script tags."""
    for script in soup.find_all("script", type="application/ld+json"):
        text = (script.string or script.get_text() or "").strip()
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict) and item.get("@type") == "JobPosting":
                return item
    return None


def extract_salary_from_jsonld(item: Dict) -> str:
    """Parse salary from JSON-LD baseSalary field."""
    base_salary = item.get("baseSalary")
    if not base_salary:
        return "Unknown"

    if isinstance(base_salary, str):
        s = clean_text(base_salary)
        if not s or s.lower() in ("negotiable", "thỏa thuận", "thương lượng", "thoả thuận"):
            return "Negotiable"
        return s

    if isinstance(base_salary, dict):
        currency = base_salary.get("currency", "")
        value = base_salary.get("value")
        if isinstance(value, dict):
            # Check for actual value string like "Negotiable"
            raw_val = value.get("value", "")
            if isinstance(raw_val, str) and re.search(r'[a-zA-Z]', raw_val):
                low = raw_val.lower()
                if any(w in low for w in ("negotiable", "thỏa thuận", "thương lượng", "thoả thuận")):
                    return "Negotiable"
                return clean_text(raw_val)
            # Numeric range
            min_v = value.get("minValue")
            max_v = value.get("maxValue")
            if min_v and max_v:
                return f"{min_v}-{max_v} {currency}".strip()
            if min_v:
                return f"{min_v} {currency}".strip()
            if max_v:
                return f"{max_v} {currency}".strip()
            if raw_val:
                return f"{raw_val} {currency}".strip()

    return "Unknown"


# =====================================
# Scraping
# =====================================

def get_job_urls_from_page(page: int) -> List[str]:
    """Fetch one listing page and return all job detail URLs found."""
    sep = "&" if "?" in LISTING_URL else "?"
    url = f"{LISTING_URL}{sep}page={page}"
    print(f"  Fetching listing page {page}: {url}")
    driver = get_thread_driver()
    try:
        driver.get(url)
        time.sleep(6 + random.uniform(0, 1.5))
        # Scroll to trigger lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/detail-jobs/" in href:
                full = href if href.startswith("http") else BASE + href
                if full not in _seen_urls:
                    urls.append(full)
        return urls
    except Exception as e:
        print(f"  Error fetching page {page}: {e}")
        return []


def scrape_job_detail(url: str) -> Optional[Dict]:
    """Scrape a single job detail page. Returns a dict with all fields on one line each."""
    driver = get_thread_driver()
    try:
        driver.get(url)
        time.sleep(5 + random.uniform(0, 1))
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # ---- Parse JSON-LD (primary source) ----
        jld = parse_jsonld(soup)

        # 1. TITLE
        title = ""
        if jld:
            title = clean_text(jld.get("title", ""))
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = clean_text(h1.get_text())
        if not title:
            title = clean_text(soup.title.string if soup.title else "")

        # 2. COMPANY
        company = ""
        if jld:
            org = jld.get("hiringOrganization", {})
            if isinstance(org, dict):
                company = clean_text(org.get("name", ""))
        if not company:
            # Try page meta or structured data
            for sel in ["[class*='company-name']", "[class*='employer-name']", "h2"]:
                el = soup.select_one(sel)
                if el:
                    company = clean_text(el.get_text())
                    break

        # 3. SALARY — from JSON-LD (no login needed for JSON-LD)
        salary = "Unknown"
        if jld:
            salary = extract_salary_from_jsonld(jld)
        # If still Unknown, try page text for common patterns
        if salary == "Unknown":
            page_text = soup.get_text(" ")
            for line in page_text.splitlines():
                line = line.strip()
                if re.search(r'\d[\d,\.]*\s*(?:usd|triệu|million|\$)', line, re.IGNORECASE):
                    salary = clean_text(line)[:100]
                    break
                if any(w in line.lower() for w in ("thỏa thuận", "thương lượng", "negotiable")):
                    salary = "Negotiable"
                    break

        # 4. LOCATION — from JSON-LD address
        location = "Unknown"
        if jld:
            loc = jld.get("jobLocation")
            if isinstance(loc, list) and loc:
                loc = loc[0]
            if isinstance(loc, dict):
                addr = loc.get("address", {})
                if isinstance(addr, dict):
                    parts = []
                    for k in ["addressLocality", "addressRegion"]:
                        v = addr.get(k, "").strip()
                        if v and v.lower() != "remote":
                            parts.append(v)
                    if not parts and addr.get("addressRegion"):
                        parts.append(addr["addressRegion"].strip())
                    if parts:
                        location = ", ".join(parts)
                    elif addr.get("addressLocality"):
                        location = addr["addressLocality"].strip()
            if location == "Unknown" and isinstance(loc, dict):
                loc_name = loc.get("name", "")
                if loc_name:
                    location = clean_text(loc_name)

        # 5. DESCRIPTION — from JSON-LD or page, collapsed to 1 line
        description = ""
        if jld and jld.get("description"):
            description = clean_text(jld["description"])
        if not description:
            # Try to find the main content area
            for sel in ["[class*='job-description']", "[class*='content']", "article", "main"]:
                el = soup.select_one(sel)
                if el:
                    description = clean_text(el.get_text())
                    if len(description) > 100:
                        break

        # 6. SKILLS — from JSON-LD skills field first, then extract from text
        skills = ""
        if jld:
            raw_skills = jld.get("skills", "")
            if isinstance(raw_skills, list):
                skills = ", ".join([s.strip() for s in raw_skills if s.strip()])
            elif isinstance(raw_skills, str) and raw_skills.strip():
                skills = raw_skills.strip()

        # Also extract known skills from description + title
        extracted = extract_skills_from_text(f"{title} {description}")
        if extracted and not skills:
            skills = extracted
        elif extracted and skills:
            # Merge without duplicates
            existing = set(s.strip().lower() for s in skills.split(","))
            for s in extracted.split(", "):
                if s.lower() not in existing:
                    skills += f", {s}"

        # 7. EXPERIENCE — from JSON-LD experienceRequirements or page text
        experience = "Unknown"
        if jld:
            exp_req = jld.get("experienceRequirements", "")
            if isinstance(exp_req, str) and exp_req.strip():
                experience = clean_text(exp_req)
        if experience == "Unknown":
            # Try to find experience from page text
            page_text = soup.get_text(" ").lower()
            # Look for patterns like "1-2 years", "3-5 years", etc.
            exp_patterns = [
                r'(\d+)\s*-\s*(\d+)\s*(?:năm|year|yrs?)',
                r'(?:ít nhất|at least|minimum)?\s*(\d+)\s*(?:năm|year|yrs?)',
            ]
            for pattern in exp_patterns:
                match = re.search(pattern, page_text)
                if match:
                    experience = clean_text(match.group(0))
                    break

        # 8. JOB LEVEL — from JSON-LD or page text
        job_level = "Unknown"
        level_keywords = {
            "Fresher": ["fresher", "mới ra trường", "graduate", "entry", "entry-level"],
            "Junior": ["junior", "junior developer", "junior engineer"],
            "Middle": ["middle", "mid-level", "senior developer", "medium"],
            "Senior": ["senior", "senior developer", "senior engineer", "lead", "principal"],
            "Architect": ["architect", "solutions architect", "lead architect"],
        }
        
        page_text_lower = soup.get_text(" ").lower()
        title_lower = title.lower()
        
        # Try to find level from title first
        for level_name, keywords in level_keywords.items():
            for keyword in keywords:
                if keyword in title_lower:
                    job_level = level_name
                    break
            if job_level != "Unknown":
                break
        
        # If not found in title, try page text
        if job_level == "Unknown":
            for level_name, keywords in level_keywords.items():
                for keyword in keywords:
                    if keyword in page_text_lower:
                        job_level = level_name
                        break
                if job_level != "Unknown":
                    break

        # 9. URL + CRAWL TIME
        job_url = url
        crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        return {
            "title": title,
            "company": company,
            "salary": salary,
            "location": location,
            "description": description,
            "skills": skills,
            "experience": experience,
            "job_level": job_level,
            "url": job_url,
            "crawl_time": crawl_time,
        }

    except Exception as e:
        print(f"  Error scraping {url}: {e}")
        return None


# =====================================
# Save helpers
# =====================================

def autosave():
    """Write current jobs to autosave CSV (utf-8-sig for Excel compat)."""
    try:
        df = pd.DataFrame(_all_jobs, columns=COLUMNS)
        df.to_csv(AUTOSAVE_PATH, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    except Exception as e:
        print(f"Autosave error: {e}")


def save_final():
    """Write final CSV."""
    try:
        df = pd.DataFrame(_all_jobs, columns=COLUMNS)
        df.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
        print(f"Saved {len(_all_jobs)} jobs to {FINAL_CSV}")
    except Exception as e:
        print(f"Final save error: {e}")


# =====================================
# Worker
# =====================================

def process_job(url: str):
    job = scrape_job_detail(url)
    if job:
        with DRIVER_LOCK:
            _all_jobs.append(job)
            _stats["success"] += 1
            count = len(_all_jobs)
            if count % AUTOSAVE_INTERVAL == 0:
                autosave()
                print(f"  Autosaved {count} jobs...")
    else:
        with DRIVER_LOCK:
            _stats["errors"] += 1


# =====================================
# Main
# =====================================

def main():
    print("=" * 60)
    print("TopDev Scraper — matching vnwork.csv format")
    print(f"Target: {LISTING_URL}")
    print("=" * 60)

    # Phase 1: Collect all job URLs
    print("\n[Phase 1] Collecting job URLs from listing pages...")
    all_job_urls: List[str] = []

    for page in range(1, MAX_PAGES + 1):
        urls = get_job_urls_from_page(page)
        new_urls = [u for u in urls if u not in _seen_urls]
        if not new_urls:
            print(f"  Page {page}: no new URLs, stopping pagination.")
            break
        for u in new_urls:
            _seen_urls.add(u)
        all_job_urls.extend(new_urls)
        print(f"  Page {page}: +{len(new_urls)} jobs (total: {len(all_job_urls)})")
        if len(all_job_urls) >= 1000:
            print(f"  Reached 1000 URLs, stopping.")
            break

    _stats["total_urls"] = len(all_job_urls)
    print(f"\n[Phase 1] Done. Collected {_stats['total_urls']} unique job URLs.\n")

    if not all_job_urls:
        print("No URLs found. Exiting.")
        quit_all_drivers()
        return

    # Phase 2: Scrape each job detail
    print(f"[Phase 2] Scraping {_stats['total_urls']} job pages with {MAX_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_job, url): url for url in all_job_urls}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                future.result()
            except Exception as e:
                url = futures[future]
                print(f"  [Worker error] {url}: {e}")
            if i % 50 == 0:
                elapsed = (datetime.now() - _stats["start_time"]).seconds
                print(f"  Progress: {i}/{_stats['total_urls']} | success={_stats['success']} | errors={_stats['errors']} | {elapsed}s")

    # Final save
    save_final()
    autosave()

    elapsed = (datetime.now() - _stats["start_time"]).seconds
    print("\n" + "=" * 60)
    print(f"DONE! {_stats['success']} jobs saved. Errors: {_stats['errors']}. Time: {elapsed}s")
    print(f"Output: {FINAL_CSV}")
    print("=" * 60)

    quit_all_drivers()


if __name__ == "__main__":
    main()
