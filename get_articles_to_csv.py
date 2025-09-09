#!/usr/bin/env python3
import os
import sys
import csv
import math
import time
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Event Registry (NewsAPI.ai) articles for the last N days and write to CSV."
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        default=os.getenv("EVENT_REGISTRY_API_KEY"),
        help="Event Registry API key. Or set env EVENT_REGISTRY_API_KEY.",
    )
    parser.add_argument(
        "--output",
        default="news_articles.csv",
        help="Output CSV file path.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Number of days to look back from today (UTC). Ignored if date_start/date_end provided.",
    )
    parser.add_argument(
        "--date-start",
        dest="date_start",
        default=None,
        help="Override start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--date-end",
        dest="date_end",
        default=None,
        help="Override end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--lang",
        default="eng",
        help="Article language code (e.g., eng, deu, fra).",
    )
    parser.add_argument(
        "--keyword",
        default=None,
        help="Optional keyword filter to narrow results.",
    )
    parser.add_argument(
        "--query",
        default=None,
        help="Optional query string to narrow results (advanced).",
    )
    parser.add_argument(
        "--source-uri",
        dest="source_uri",
        default=None,
        help="Comma-separated list of sourceUri filters.",
    )
    parser.add_argument(
        "--category-uri",
        dest="category_uri",
        default=None,
        help="Comma-separated list of categoryUri filters.",
    )
    parser.add_argument(
        "--articles-count",
        dest="articles_count",
        type=int,
        default=100,
        help="Articles per page (max 100).",
    )
    parser.add_argument(
        "--max-pages",
        dest="max_pages",
        type=int,
        default=50,
        help="Safety cap on number of pages to fetch (0 for unlimited).",
    )
    parser.add_argument(
        "--page-delay",
        dest="page_delay",
        type=float,
        default=0.5,
        help="Seconds to sleep between page requests.",
    )
    parser.add_argument(
        "--body-len",
        dest="body_len",
        type=int,
        default=-1,
        help="articleBodyLen parameter (-1 for full text, or a positive snippet length).",
    )
    parser.add_argument(
        "--skip-duplicates",
        dest="skip_duplicates",
        action="store_true",
        default=True,
        help="Skip duplicate articles (isDuplicateFilter=skipDuplicates).",
    )
    parser.add_argument(
        "--no-skip-duplicates",
        dest="skip_duplicates",
        action="store_false",
        help="Do not skip duplicates (keepAll).",
    )
    parser.add_argument(
        "--endpoint",
        default="https://eventregistry.org/api/v1/article/getArticles",
        help="API endpoint URL.",
    )
    return parser.parse_args()


def compute_dates(days: int, date_start: Optional[str], date_end: Optional[str]) -> Tuple[str, str]:
    if date_start and date_end:
        return date_start, date_end
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=max(days, 1))
    return start.isoformat(), today.isoformat()


def build_payload(args: argparse.Namespace, page: int) -> dict:
    payload = {
        "resultType": "articles",
        "articlesPage": page,
        "articlesCount": args.articles_count,
        "articlesSortBy": "date",
        "articlesSortByAsc": False,
        "articleBodyLen": args.body_len,
        "dateStart": args.date_start,
        "dateEnd": args.date_end,
        "includeArticleTitle": True,
        "includeArticleBasicInfo": True,
        "includeArticleBody": True,
        "includeArticleConcepts": True,
        "includeArticleCategories": True,
        "includeSourceTitle": True,
        "includeConceptLabel": True,
        "conceptLang": args.lang,
    }
    if args.skip_duplicates:
        payload["isDuplicateFilter"] = "skipDuplicates"
    else:
        payload["isDuplicateFilter"] = "keepAll"
    if args.lang:
        payload["lang"] = args.lang
    if args.keyword:
        payload["keyword"] = args.keyword
    if args.query:
        payload["query"] = args.query
    if args.source_uri:
        payload["sourceUri"] = [s.strip() for s in args.source_uri.split(",") if s.strip()]
    if args.category_uri:
        payload["categoryUri"] = [s.strip() for s in args.category_uri.split(",") if s.strip()]
    return payload


def request_page(
    session: requests.Session,
    endpoint: str,
    payload: dict,
    api_key: str,
    max_retries: int = 5,
) -> dict:
    merged = dict(payload)
    merged["apiKey"] = api_key
    backoff = 1.0
    for attempt in range(max_retries):
        try:
            resp = session.post(endpoint, json=merged, timeout=60)
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2)
            continue
        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                raise RuntimeError("Invalid JSON in API response")
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_retries - 1:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2)
            continue
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
    raise RuntimeError("Max retries exceeded")


def extract_tags(article: dict, lang: str) -> List[str]:
    tags: List[str] = []

    def normalize_label(item: object) -> str:
        # Item might be a dict with label/uri, or already a string
        if isinstance(item, str):
            return item
        if isinstance(item, dict):
            label_container = item.get("label") or {}
            if isinstance(label_container, dict):
                # Try requested language, then English, then any first value
                label = label_container.get(lang) or label_container.get("eng")
                if not label and label_container:
                    try:
                        # Pick any first label if available
                        label = next(iter(label_container.values()))
                    except StopIteration:
                        label = None
                if label:
                    return str(label)
            # Fallbacks
            if item.get("uri"):
                return str(item.get("uri"))
            if item.get("label") and isinstance(item.get("label"), str):
                return str(item.get("label"))
        return ""

    for concept in (article.get("concepts") or []):
        label = normalize_label(concept)
        if label:
            tags.append(label)

    for category in (article.get("categories") or []):
        label = normalize_label(category)
        if label:
            tags.append(label)

    return tags


def main() -> None:
    args = parse_args()
    if not args.api_key:
        print("ERROR: Provide API key via --api-key or EVENT_REGISTRY_API_KEY env var.", file=sys.stderr)
        sys.exit(1)

    # Compute date range
    start, end = compute_dates(args.days, args.date_start, args.date_end)
    args.date_start, args.date_end = start, end

    # Safety note
    if args.max_pages == 0:
        print("Warning: --max-pages is 0 (unlimited). This may fetch a very large number of articles.", file=sys.stderr)

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    total_rows = 0
    page = 1
    # Ensure parent directory exists if output includes subdirectories
    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Headline", "Link URL", "Outlet", "Text", "Tags/keywords"])

        while True:
            payload = build_payload(args, page)
            data = request_page(session, args.endpoint, payload, args.api_key)

            articles_section = data.get("articles") or {}
            results = articles_section.get("results") or []
            if not results:
                break

            for article in results:
                headline = article.get("title") or ""
                link_url = article.get("url") or ""
                outlet = (article.get("source") or {}).get("title") or ""
                text = article.get("body") or ""
                tags = extract_tags(article, args.lang)
                writer.writerow([headline, link_url, outlet, text, "; ".join(tags)])
                total_rows += 1

            # Determine if we should continue
            total_results = (articles_section.get("totalResults") if isinstance(articles_section, dict) else None)
            total_pages = None
            if isinstance(total_results, int) and args.articles_count > 0:
                total_pages = math.ceil(total_results / args.articles_count)

            if args.max_pages and page >= args.max_pages:
                break
            if total_pages and page >= total_pages:
                break

            page += 1
            if args.page_delay > 0:
                time.sleep(args.page_delay)

    print(f"Wrote {total_rows} rows to {args.output}")


if __name__ == "__main__":
    main()


