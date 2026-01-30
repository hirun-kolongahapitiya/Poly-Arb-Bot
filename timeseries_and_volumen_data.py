#!/usr/bin/env python3
import argparse
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"


def request_json(url: str, params: Dict[str, Any]) -> Any:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def extract_price_points(payload: Any) -> List[Dict[str, Any]]:
    items: Iterable[Any]
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = (
            payload.get("history")
            or payload.get("prices")
            or payload.get("data")
            or payload.get("priceHistory")
            or payload.get("price_history")
            or []
        )
    else:
        items = []

    points: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ts = item.get("t") if "t" in item else item.get("timestamp")
        price = item.get("p") if "p" in item else item.get("price")
        if ts is None or price is None:
            continue
        try:
            ts_value = int(ts)
            price_value = float(price)
        except (TypeError, ValueError):
            continue
        points.append({"timestamp": ts_value, "price": price_value})
    return points


def fetch_price_history_df(
    token_id: str,
    interval: Optional[str],
    start_ts: Optional[int],
    end_ts: Optional[int],
    fidelity: Optional[int],
) -> pd.DataFrame:
    params: Dict[str, Any] = {"market": token_id}
    if interval:
        params["interval"] = interval
    else:
        if start_ts is not None:
            params["startTs"] = start_ts
        if end_ts is not None:
            params["endTs"] = end_ts
    if fidelity is not None:
        params["fidelity"] = fidelity

    payload = request_json(f"{DATA_API}/prices-history", params)
    points = extract_price_points(payload)
    df = pd.DataFrame(points)
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(None)
    return df.sort_values("timestamp")


def normalize_tokens(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    tokens: List[str] = []
    for value in values:
        token = str(value or "").strip()
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def normalize_outcomes(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    return [str(value or "").strip() for value in values]


def fetch_gamma_markets(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = request_json(f"{GAMMA_API}/markets", params)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("data") or payload.get("markets") or []
    return []


def fetch_gamma_by_slug(slug: str) -> List[Dict[str, Any]]:
    return fetch_gamma_markets({"slug": slug})


def fetch_gamma_markets_page(limit: int, offset: int) -> List[Dict[str, Any]]:
    return fetch_gamma_markets({"limit": limit, "offset": offset})


def parse_slug_timestamp(slug: str) -> Optional[int]:
    match = re.search(r"-(\d{9,12})$", slug)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def parse_date_to_seconds(value: Any) -> Optional[int]:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def get_market_timestamp_seconds(market: Dict[str, Any]) -> Optional[int]:
    slug = str(market.get("slug") or "").strip()
    slug_ts = parse_slug_timestamp(slug) if slug else None
    if slug_ts is not None:
        return slug_ts
    return parse_date_to_seconds(market.get("endDateIso") or market.get("endDate"))


def build_market_choices(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    tokens = normalize_tokens(
        market.get("clobTokenIds")
        or market.get("clob_token_ids")
        or market.get("clobTokenIDs")
    )
    outcomes = normalize_outcomes(market.get("outcomes"))
    choices: List[Dict[str, Any]] = []
    for index, token_id in enumerate(tokens):
        choices.append(
            {
                "token_id": token_id,
                "outcome": outcomes[index] if index < len(outcomes) else None,
                "outcome_index": index,
            }
        )
    return choices


def select_market_choices(
    choices: List[Dict[str, Any]],
    outcome: Optional[str],
    outcome_index: Optional[int],
) -> List[Dict[str, Any]]:
    if outcome_index is not None:
        filtered = [choice for choice in choices if choice.get("outcome_index") == outcome_index]
        if not filtered:
            raise ValueError(f"No outcome_index {outcome_index} found")
        return filtered
    if outcome:
        normalized = outcome.strip().lower()
        filtered = [
            choice
            for choice in choices
            if (choice.get("outcome") or "").strip().lower() == normalized
        ]
        if not filtered:
            raise ValueError(f"No outcome matching {outcome!r} found")
        return filtered
    return choices


def resolve_markets_from_slug(
    slug: str, outcome: Optional[str], outcome_index: Optional[int]
) -> List[Dict[str, Any]]:
    markets = fetch_gamma_by_slug(slug)
    results: List[Dict[str, Any]] = []
    seen: set = set()
    for market in markets:
        if not isinstance(market, dict):
            continue
        for choice in select_market_choices(build_market_choices(market), outcome, outcome_index):
            key = (slug, choice["token_id"], choice.get("outcome_index"))
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "slug": slug,
                    "token_id": choice["token_id"],
                    "outcome": choice.get("outcome"),
                    "outcome_index": choice.get("outcome_index"),
                    "market_timestamp": get_market_timestamp_seconds(market),
                    "end_date": market.get("endDateIso") or market.get("endDate"),
                }
            )
    return results


def is_timestamp_in_range(
    value: Optional[int], start_ts: Optional[int], end_ts: Optional[int]
) -> bool:
    if value is None:
        return start_ts is None and end_ts is None
    if start_ts is not None and value < start_ts:
        return False
    if end_ts is not None and value > end_ts:
        return False
    return True


def resolve_markets_by_prefix(
    prefix: str,
    start_ts: Optional[int],
    end_ts: Optional[int],
    max_markets: int,
    scan_limit: int,
    page_size: int,
) -> Tuple[List[Dict[str, Any]], int]:
    results: List[Dict[str, Any]] = []
    seen: set = set()
    prefix_lower = prefix.lower()
    offset = 0
    scanned = 0
    while scanned < scan_limit:
        batch = fetch_gamma_markets_page(page_size, offset)
        if not batch:
            break
        scanned += len(batch)
        for market in batch:
            if not isinstance(market, dict):
                continue
            slug = str(market.get("slug") or "").strip()
            if not slug or not slug.lower().startswith(prefix_lower):
                continue
            market_ts = get_market_timestamp_seconds(market)
            if (start_ts is not None or end_ts is not None) and not is_timestamp_in_range(
                market_ts, start_ts, end_ts
            ):
                continue
            for choice in build_market_choices(market):
                key = (slug, choice["token_id"], choice.get("outcome_index"))
                if key in seen:
                    continue
                seen.add(key)
                results.append(
                    {
                        "slug": slug,
                        "token_id": choice["token_id"],
                        "outcome": choice.get("outcome"),
                        "outcome_index": choice.get("outcome_index"),
                        "market_timestamp": market_ts,
                        "end_date": market.get("endDateIso") or market.get("endDate"),
                    }
                )
                if len(results) >= max_markets:
                    break
            if len(results) >= max_markets:
                break
        if len(batch) < page_size or len(results) >= max_markets:
            break
        offset += len(batch)
    return results, scanned


def derive_prefix_from_slug(slug: str) -> str:
    return re.sub(r"-\d{9,12}$", "", slug)


def build_output_name(label: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "-", label.strip()) or "market"
    date_tag = datetime.utcnow().strftime("%Y-%m-%d")
    return f"price-history-{safe}-{date_tag}.xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Polymarket price history and export to Excel."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--token-id", help="CLOB token id (fetch directly)")
    group.add_argument("--slug", help="Market slug (Gamma lookup)")
    group.add_argument("--prefix", help="Slug prefix to collect multiple markets")
    parser.add_argument("--expand-prefix", action="store_true", help="Use slug prefix mode")
    parser.add_argument("--interval", help="Interval (e.g. 30d, 7d, 1h)")
    parser.add_argument("--start-ts", type=int, help="Start timestamp (unix seconds)")
    parser.add_argument("--end-ts", type=int, help="End timestamp (unix seconds)")
    parser.add_argument("--fidelity", type=int, help="Fidelity in minutes")
    parser.add_argument("--outcome", help="Outcome label to select (Yes/No)")
    parser.add_argument("--outcome-index", type=int, help="Outcome index to select")
    parser.add_argument("--max-markets", type=int, default=200, help="Max markets to fetch")
    parser.add_argument("--scan-limit", type=int, default=2000, help="Max markets to scan")
    parser.add_argument("--page-size", type=int, default=200, help="Gamma page size")
    parser.add_argument("--output", help="Output Excel path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    entries: List[Dict[str, Any]] = []
    label = ""

    if args.prefix:
        label = args.prefix
        entries, _ = resolve_markets_by_prefix(
            prefix=args.prefix,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            max_markets=args.max_markets,
            scan_limit=args.scan_limit,
            page_size=args.page_size,
        )
    elif args.slug and args.expand_prefix:
        prefix = derive_prefix_from_slug(args.slug)
        label = prefix
        entries, _ = resolve_markets_by_prefix(
            prefix=prefix,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            max_markets=args.max_markets,
            scan_limit=args.scan_limit,
            page_size=args.page_size,
        )
    elif args.slug:
        label = args.slug
        entries = resolve_markets_from_slug(args.slug, args.outcome, args.outcome_index)
    elif args.token_id:
        label = args.token_id
        entries = [
            {
                "slug": None,
                "token_id": args.token_id,
                "outcome": args.outcome,
                "outcome_index": args.outcome_index,
                "market_timestamp": None,
                "end_date": None,
            }
        ]

    if not entries:
        raise SystemExit("No markets resolved for the provided input.")

    frames: List[pd.DataFrame] = []
    for entry in entries:
        token_id = str(entry["token_id"])
        df = fetch_price_history_df(
            token_id=token_id,
            interval=args.interval,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            fidelity=args.fidelity,
        )
        if df.empty:
            continue
        df["slug"] = entry.get("slug")
        df["token_id"] = token_id
        df["outcome"] = entry.get("outcome")
        df["outcome_index"] = entry.get("outcome_index")
        df["market_timestamp"] = entry.get("market_timestamp")
        df["end_date"] = entry.get("end_date")
        frames.append(df)

    if not frames:
        raise SystemExit("No price history returned for resolved markets.")

    history = pd.concat(frames, ignore_index=True)
    history = history.sort_values(["slug", "token_id", "timestamp"])

    output_path = args.output or build_output_name(label)
    markets_df = pd.DataFrame(entries)
    with pd.ExcelWriter(output_path) as writer:
        history.to_excel(writer, sheet_name="history", index=False)
        markets_df.to_excel(writer, sheet_name="markets", index=False)

    print(f"Saved {len(history)} rows to {output_path}")


if __name__ == "__main__":
    main()
