import json
import time
import logging
import boto3
import requests
from datetime import date, timedelta, timezone, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wistia")
log.setLevel(logging.INFO)
logging.Formatter.converter = time.gmtime

BASE_URL = "https://api.wistia.com/v1"
MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]
BUCKET = "wistia-data-222238779166"
MAX_PAGES = 50
# Visitors: Wistia returns 504 Gateway Timeout for 100 per page (tested Nov 9, 2026); 50 returns in ~2 s
VISITORS_PER_PAGE = 50


# ---------- Token ----------
def get_token():
    client = boto3.client("secretsmanager", region_name="us-east-1")
    response = client.get_secret_value(SecretId="wistia/api-token")
    secret = json.loads(response["SecretString"])
    return secret["api-token"]


# ---------- API call with retries ----------
def call_wistia(token, url, max_retries=3):
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as error:
            log.warning(f"attempt {attempt}: network error: {error}")
        else:
            if response.status_code == 200:
                return response.json()
            if response.status_code in (401, 404):
                log.error(f"error {response.status_code} (not retrying): {url}")
                return None
            log.warning(f"attempt {attempt}: error {response.status_code}")

        if attempt < max_retries:
            wait = 2 ** attempt
            log.info(f"waiting {wait} seconds before retry")
            time.sleep(wait)
    log.error(f"giving up after {max_retries} attempts: {url}")
    return None


# ---------- 1. Media stats (lifetime) ----------
def get_media_stat(token, media_id):
    url = f"{BASE_URL}/stats/medias/{media_id}.json"
    return call_wistia(token, url)


# ---------- 2. Media metadata ----------
def get_media_metadata(token, media_id):
    url = f"{BASE_URL}/medias/{media_id}.json"
    return call_wistia(token, url)


# ---------- 3. Daily stats ----------
def get_daily_stats(token, media_id, start_date, end_date):
    url = f"{BASE_URL}/stats/medias/{media_id}/by_date.json?start_date={start_date}&end_date={end_date}"
    return call_wistia(token, url)


# ---------- 4. Visitors (paginated) ----------
def get_visitors_page(token, page, per_page):
    url = f"{BASE_URL}/stats/visitors.json?page={page}&per_page={per_page}"
    return call_wistia(token, url)


def get_all_visitors(token, per_page=VISITORS_PER_PAGE, max_pages=MAX_PAGES, since=None):
    all_visitors = []
    page = 1
    stop = False

    while page <= max_pages and not stop:
        rows = get_visitors_page(token, page, per_page)
        if rows is None:
            log.error(f"visitors paging failed on page {page}")
            return None
        if not rows:
            break
        for visitor in rows:
            if since is not None and visitor["last_active_at"] <= since:
                stop = True
                break
            all_visitors.append(visitor)
        log.info(f"visitors page {page}: {len(rows)} rows")
        page += 1
    return all_visitors


# ---------- 5. Events (paginated) ----------
def get_events_page(token, media_id, page, per_page):
    url = f"{BASE_URL}/stats/events.json?media_id={media_id}&page={page}&per_page={per_page}"
    return call_wistia(token, url)


def get_all_events(token, media_id, per_page=100, max_pages=MAX_PAGES, since=None):
    all_events = []
    page = 1
    stop = False

    while page <= max_pages and not stop:
        rows = get_events_page(token, media_id, page, per_page)
        if rows is None:
            log.error(f"events paging failed on page {page}")
            return None
        if not rows:
            break
        for event in rows:
            if since is not None and event["received_at"] <= since:
                stop = True
                break
            all_events.append(event)
        log.info(f"events page {page}: {len(rows)} rows")
        page += 1
    return all_events


# ---------- Save to S3 Bronze ----------
def save_to_bronze(data, endpoint, load_date, file_name):
    s3 = boto3.client("s3", region_name="us-east-1")
    key = f"bronze/{endpoint}/load_date={load_date}/{file_name}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    log.info(f"saved s3://{BUCKET}/{key}")


# ---------- Watermarks (DynamoDB) ----------
def get_watermark(endpoint):
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("wistia_watermark")
    response = table.get_item(Key={"endpoint": endpoint})
    item = response.get("Item")
    if item:
        return item["last_value"]
    return None


def set_watermark(endpoint, value):
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("wistia_watermark")
    table.put_item(Item={"endpoint": endpoint, "last_value": value})
    log.info(f"watermark set: {endpoint} = {value}")

# ---------- The pipeline: all 5 endpoints ----------
def run():
    token = get_token()
    failures = []
    now = datetime.now(timezone.utc)
    load_date = now.date().isoformat()
    end_date = (now.date() - timedelta(days=1)).isoformat()
    run_id = now.strftime("%H%M%S")
    log.info(f"run started: load_date={load_date} end_date={end_date} run_id={run_id}")

    for media_id in MEDIA_IDS:
        stats = get_media_stat(token, media_id)
        if stats is not None:
            save_to_bronze(stats, "media_stats", load_date, media_id)
            log.info(f"{media_id} stats: {stats['play_count']} plays, {stats['load_count']} loads")
        else:
            failures.append(f"{media_id} stats")

        metadata = get_media_metadata(token, media_id)
        if metadata is None:
            log.error(f"{media_id} metadata failed, skipping this media")
            failures.append(f"{media_id} metadata")
            continue
        save_to_bronze(metadata, "media", load_date, media_id)
        log.info(f"{media_id} metadata: {metadata['name']} updated {metadata['updated']}")

        wm_key = f"daily_stats#{media_id}"
        last_day = get_watermark(wm_key)
        if last_day is None:
            start_date = metadata["created"][:10]
        else:
            start_date = (date.fromisoformat(last_day) + timedelta(days=1)).isoformat()
        log.info(f"{media_id} daily range {start_date} -> {end_date}")

        if start_date > end_date:
            log.info(f"{media_id} daily stats already up to date")
        else:
            daily = get_daily_stats(token, media_id, start_date, end_date)
            if daily is None:
                log.error(f"{media_id} daily stats failed, bookmark not moved")
                failures.append(f"{media_id} daily_stats")
            else:
                save_to_bronze(daily, "daily_stats", load_date, f"{media_id}_{run_id}")
                set_watermark(wm_key, end_date)
                log.info(f"{media_id} daily rows: {len(daily)}")

        ev_key = f"events#{media_id}"
        last_event = get_watermark(ev_key)
        log.info(f"{media_id} events since: {last_event}")
        events = get_all_events(token, media_id, per_page=100, max_pages=MAX_PAGES, since=last_event)
        if events is None:
            log.error(f"{media_id} events failed, bookmark not moved")
            failures.append(f"{media_id} events")
        else:
            if events:
                save_to_bronze(events, "events", load_date, f"{media_id}_{run_id}")
                set_watermark(ev_key, events[0]["received_at"])
            log.info(f"{media_id} new events: {len(events)}")

    last_visitor = get_watermark("visitors")
    log.info(f"visitors since: {last_visitor}")
    visitors = get_all_visitors(token, per_page=VISITORS_PER_PAGE, max_pages=MAX_PAGES, since=last_visitor)
    if visitors is None:
        log.error("visitors failed, bookmark not moved")
        failures.append("visitors")
    else:
        if visitors:
            save_to_bronze(visitors, "visitors", load_date, f"visitors_{run_id}")
            set_watermark("visitors", visitors[0]["last_active_at"])
        log.info(f"new visitors: {len(visitors)}")

    if failures:
        log.error(f"run finished with failures: {failures}")
        raise RuntimeError(f"failed: {failures}")
    log.info("run finished")


# ---------- Entry points ----------
def lambda_handler(event, context):
    # AWS Lambda calls this function
    run()
    return {"status": "ok"}


if __name__ == "__main__":
    # Running on the laptop: python ingestion\wistia_client.py
    run()