import requests
import pandas as pd
import os
import re
import shutil
import subprocess
import time
import random
import threading
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from bs4 import BeautifulSoup

# =====================================
# KHÓA TOÀN CỤC
# tránh chromedriver conflict
# =====================================

driver_lock = threading.Lock()


def detect_chrome_binary_and_version():

    candidates = [
        shutil.which("chrome"),
        shutil.which("chrome.exe"),
        shutil.which("google-chrome"),
        os.path.join(
            os.environ.get("ProgramFiles", ""),
            "Google",
            "Chrome",
            "Application",
            "chrome.exe"
        ),
        os.path.join(
            os.environ.get("ProgramFiles(x86)", ""),
            "Google",
            "Chrome",
            "Application",
            "chrome.exe"
        ),
        os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "Google",
            "Chrome",
            "Application",
            "chrome.exe"
        )
    ]

    for candidate in candidates:

        if not candidate or not os.path.exists(candidate):
            continue

        try:
            output = subprocess.check_output(
                [candidate, "--version"],
                text=True,
                stderr=subprocess.STDOUT
            ).strip()

            match = re.search(r"(\d+)\.", output)

            if match:
                return candidate, int(match.group(1))

        except Exception:
            continue

    return None, None


def safe_quit_driver(driver):

    if not driver:
        return

    try:
        driver.quit()
    except Exception as e:
        print("DRIVER QUIT WARNING:", e)
    finally:
        try:
            driver.quit = lambda *args, **kwargs: None
        except Exception:
            pass

# =====================================
# LƯU TRỮ
# =====================================

all_jobs = []

# =====================================
# LẤY URL VIỆC LÀM
# =====================================

job_urls = []

# 100 trang * 20 job/trang ~= 2000 job
for page in range(1, 101):

    print(f"GET PAGE {page}")

    url = "https://ms.vietnamworks.com/job-search/v1.0/search"

    payload = {
        "query": "IT",
        "page": page,
        "hitsPerPage": 20
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/137.0.0.0 Safari/537.36"
        )
    }

    try:

        response = requests.post(
            url,
            json=payload,
            headers=headers
        )

        data = response.json()

        jobs = data.get("data", [])

        for job in jobs:

            job_url = job.get("jobUrl")

            if job_url:
                job_urls.append(job_url)

    except Exception as e:

        print("API ERROR:", e)

print("\nTOTAL URLS:", len(job_urls))

# =====================================
# TẠO DRIVER
# =====================================

def create_driver():

    with driver_lock:

        chrome_binary, chrome_major = detect_chrome_binary_and_version()
        chrome_major = chrome_major or 148

        print("CHROME DETECTED:", chrome_binary or "UNKNOWN")
        print("CHROME MAJOR:", chrome_major)

        options = Options()

        options.add_argument("--start-maximized")

        # KHÔNG chế độ ẩn (headless)
        # tránh bị phát hiện là bot

        options.add_argument(
            "--disable-blink-features=AutomationControlled"
        )

        options.add_argument("--no-sandbox")

        options.add_argument("--disable-dev-shm-usage")

        options.add_argument("--disable-gpu")

        options.add_argument(
            "--user-agent=Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        )

        if chrome_binary:
            options.binary_location = chrome_binary

        service = Service()

        driver = webdriver.Chrome(
            service=service,
            options=options
        )

        return driver

# =====================================
# CÀO VIỆC LÀM
# =====================================

def crawl_job(job_url):

    driver = None

    try:

        driver = create_driver()

        print("OPEN:", job_url)

        driver.get(job_url)

        # nghỉ ngẫu nhiên tránh bị chặn
        time.sleep(random.uniform(3, 5))

        # giả lập cuộn trang như người dùng thật
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight/2);"
        )

        time.sleep(random.uniform(1, 2))

        html = driver.page_source

        # phát hiện bị chặn
        if "403 Forbidden" in html:

            print("BỊ CHẶN")

            return None

        soup = BeautifulSoup(
            html,
            "html.parser"
        )

        # =====================================
        # TIÊU ĐỀ
        # =====================================

        title = ""

        h1 = soup.find("h1")

        if h1:
            title = h1.get_text(strip=True)

        # =====================================
        # CÔNG TY
        # =====================================

        company = ""

        company_blacklist = [
            "lưu công việc",
            "tải upzi",
            "xem thêm",
            "việc làm",
            "dành cho nhà tuyển dụng",
            "tất cả danh mục",
            "việc làm theo khu vực",
            "việc làm theo ngành nghề",
            "vietnamworks"
        ]

        company_link_candidates = soup.find_all(
            "a",
            href=re.compile(r"/nha-tuyen-dung/")
        )

        for company_element in company_link_candidates:

            text = company_element.get_text(" ", strip=True)

            if 3 < len(text) < 120:

                lowered_text = text.lower()

                if not any(term in lowered_text for term in company_blacklist):

                    company = text
                    break

        if not company:

            company_selectors = [
                ".company-name",
                ".sc-fqkvVR",
                "[class*=company]"
            ]

            for selector in company_selectors:

                company_element = soup.select_one(selector)

                if company_element:

                    text = company_element.get_text(strip=True)

                    if 3 < len(text) < 100:

                        company = text
                        break

        # =====================================
        # LƯƠNG
        # =====================================

        salary = ""

        salary_pattern = re.compile(
            r"(\$?\s?\d+[.,]?\d*\s?(triệu|VND|USD|\/tháng)?)",
            re.IGNORECASE
        )

        salary_blacklist = [
            "tuyển",
            "ứng tuyển",
            "mô tả công việc",
            "yêu cầu công việc",
            "thông tin việc làm",
            "địa điểm làm việc",
            "việc làm cùng công ty",
            "tại ",
            "để xem nhiều việc làm",
            "intern",
            "fresher",
            "dưới 3 năm kinh nghiệm",
            "tải upzi"
        ]

        salary_candidates = []

        for tag in soup.find_all(["span", "div", "p"]):

            text = tag.get_text(" ", strip=True)

            if not text or len(text) > 80:
                continue

            lowered_text = text.lower()

            if any(term in lowered_text for term in salary_blacklist):
                continue

            if "thương lượng" in lowered_text:

                salary = "Thương lượng"
                break

            if salary_pattern.search(text):

                salary = text
                break

            if any(token in lowered_text for token in ["triệu", "usd", "vnd", "/tháng", "/month"]):

                salary_candidates.append(text)

        if not salary and salary_candidates:

            salary = salary_candidates[0]

        # =====================================
        # ĐỊA ĐIỂM
        # =====================================

        location = ""

        location_mapping = {
            "Hồ Chí Minh": "HCM",
            "Hà Nội": "HN",
            "Đà Nẵng": "DN"
        }

        location_found = False

        for t in soup.stripped_strings:

            if location_found:
                break

            for source_location, normalized_location in location_mapping.items():

                if source_location in t:

                    location = normalized_location
                    location_found = True
                    break

        # =====================================
        # KỸ NĂNG
        # =====================================

        skills = []

        blacklist = [
            "việc làm",
            "đăng nhập",
            "ứng tuyển",
            "trang chủ",
            "tìm kiếm",
            "upzi",
            "lưu công việc",
            "thương lượng",
            "lượt xem"
        ]

        for tag in soup.find_all(["a", "span"]):

            text = tag.get_text(strip=True)

            if (
                2 < len(text) < 25
                and not any(char.isdigit() for char in text)
            ):

                skip = False

                for b in blacklist:

                    if b in text.lower():
                        skip = True
                        break

                if not skip and text not in skills:

                    skills.append(text)

        # =====================================
        # KẾT QUẢ
        # =====================================

        description = ""

        paragraphs = soup.find_all("p")

        description_texts = []

        for paragraph in paragraphs:

            text = paragraph.get_text(strip=True)

            if len(text) > 50:

                description_texts.append(text)

        description = " ".join(description_texts[:5])

        result = {
            "title": title,
            "company": company,
            "salary": salary,
            "location": location,
            "description": description,
            "skills": ", ".join(skills[:10]),
            "url": job_url,
            "crawl_time": datetime.now()
        }

        print("COLLECTED:", title)

        return result

    except Exception as e:

        print("CRAWL ERROR:", job_url, e)

        return None

    finally:

        safe_quit_driver(driver)

# =====================================
# ĐA LUỒNG
# =====================================

with ThreadPoolExecutor(max_workers=2) as executor:

    results = executor.map(
        crawl_job,
        job_urls
    )

    for r in results:

        if r:

            all_jobs.append(r)

            # tự động lưu
            if len(all_jobs) % 10 == 0:

                pd.DataFrame(all_jobs).to_csv(
                    "autosave.csv",
                    index=False,
                    encoding="utf-8-sig"
                )

                print("AUTOSAVED")

# =====================================
# LƯU KẾT QUẢ CUỐI
# =====================================

df = pd.DataFrame(all_jobs)

df.drop_duplicates(inplace=True)

df.to_csv(
    "vnwork.csv",
    index=False,
    encoding="utf-8-sig"
)

print("\nDONE")
print("TOTAL:", len(df))