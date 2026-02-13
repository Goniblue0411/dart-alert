#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bot.py - DART Rights Issue Telegram Alert (UI like screenshot)
- Extract: 유상증자 관련 공시
- Exclude: 제3자배정 유상증자 (제3자/특정인 배정 포함)
- Telegram: card-like text + inline button "📄 DART 열기"
- Dedup: state.json

ENV (required)
  DART_API_KEY
  TG_BOT_TOKEN
  TG_CHAT_ID

ENV (optional)
  LOOKBACK_DAYS=3
  MARKET_CLASSES="Y,K,N"     # Y:유가, K:코스닥, N:코넥스
  POLL_SLEEP_SEC=0           # 0이면 1회 실행, 30~300이면 계속 폴링
  STATE_PATH="state.json"
"""

from __future__ import annotations

import os
import re
import json
import time
import html as html_lib
from datetime import datetime, timedelta

import requests


# =========================
# ENV
# =========================
DART_API_KEY = os.getenv("4e34368459edf9be284521643b0b623f94684efe", "").strip()
TG_BOT_TOKEN = os.getenv("8337357668:AAHy1zroWzyuBzm95FNOWq_pXcaPb0sepv8", "").strip()
TG_CHAT_ID   = os.getenv("8398762332", "").strip()

LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS", "3"))
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "Y,K,N").split(",") if x.strip()]
POLL_SLEEP_SEC = int(os.getenv("POLL_SLEEP_SEC", "0"))
STATE_PATH     = os.getenv("STATE_PATH", "state.json").strip()

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_VIEW_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcpNo}"

TG_SEND_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

UA_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DART-RightsIssueBot/2.0)"}


# =========================
# Filters: exclude 3rd-party allocation
# =========================
BANNED_TITLE_TOKENS = [
    "제3자", "제삼자", "제3자배정", "제삼자배정",
    "특정인", "제3자 배정", "제 삼자 배정",
    "third party", "3rd party",
]

# 본문에서 제3자배정 흔적 탐지(공백/표기 흔들림 포함)
BANNED_BODY_PATTERNS = [
    r"제\s*3\s*자\s*배정",
    r"제\s*삼\s*자\s*배정",
    r"제3자\s*배정",
    r"제삼자\s*배정",
    r"특정인\s*배정",
    r"third\s*party\s*allocation",
    r"3rd\s*party\s*allocation",
]

RIGHTS_TITLE_KEYWORDS = [
    "유상증자",
    "유상증자결정",
    "유상증자 결정",
    "유상증자또는주식관련사채등의발행결과",
    "유상증자또는주식관련사채등의발행결과(자율공시)",
]

# 무상증자는 기본 제외 (원하면 제거)
BANNED_NON_TARGET = [
    "무상증자",
]


def _norm(s: str) -> str:
    return (s or "").strip()


def is_rights_issue_title(report_nm: str) -> bool:
    """제목 기반 1차: 유상증자 계열 포함 + 제3자/특정인 배정 토큰 제외 + 무상증자 제외"""
    report_nm = _norm(report_nm)
    if not report_nm:
        return False

    if not any(k in report_nm for k in RIGHTS_TITLE_KEYWORDS):
        # "유상증자"가 포함된 다양한 제목도 잡기 위해 보완:
        if "유상증자" not in report_nm:
            return False

    if any(x in report_nm for x in BANNED_NON_TARGET):
        return False

    low = report_nm.lower()
    if any(tok.lower() in low for tok in BANNED_TITLE_TOKENS):
        return False

    return True


def body_contains_third_party(text: str) -> bool:
    """본문(HTML/텍스트)에 제3자배정 흔적이 있으면 True"""
    text = _norm(text)
    if not text:
        return False
    for pat in BANNED_BODY_PATTERNS:
        if re.search(pat, text, flags=re.IGNORECASE):
            return True
    return False


def fetch_text(url: str, timeout: int = 12) -> str:
    r = requests.get(url, timeout=timeout, headers=UA_HEADERS)
    r.raise_for_status()
    return r.text


def fetch_dart_main_and_related(rcp_no: str) -> str:
    """
    main.do + (있으면) 관련 viewer 링크까지 일부 추가 수집
    - 제3자배정이 제목에 없고 본문에만 있는 케이스 방지용
    """
    main_url = DART_VIEW_URL.format(rcpNo=rcp_no)
    main_html = fetch_text(main_url)

    urls = {main_url}

    # main.do 안의 href를 스캔해서 viewer/보고서 링크를 최대 3개까지 추가로 가져옴
    for m in re.finditer(r'href="([^"]+)"', main_html, flags=re.IGNORECASE):
        href = m.group(1) or ""
        if not href:
            continue

        # viewer/report 관련 링크만
        if ("viewer" not in href) and ("report" not in href) and ("dsaf001" not in href):
            continue

        if href.startswith("/"):
            href = "https://dart.fss.or.kr" + href
        elif href.startswith("http"):
            pass
        else:
            continue

        if "rcpNo=" in href and rcp_no in href:
            urls.add(href)

        if len(urls) >= 4:  # main 포함 최대 4개
            break

    combined = [main_html]
    fetched = 0
    for u in list(urls):
        if u == main_url:
            continue
        if fetched >= 3:
            break
        try:
            combined.append(fetch_text(u))
            fetched += 1
        except Exception:
            continue

    return "\n\n".join(combined)


def is_allowed_rights_issue(report_nm: str, rcp_no: str) -> bool:
    """최종: 유상증자이며 제3자배정이 아닌 경우만 True"""
    if not is_rights_issue_title(report_nm):
        return False

    # 본문 확인 (실패 시 안전하게 제외)
    try:
        combined_html = fetch_dart_main_and_related(rcp_no)
    except Exception:
        return False

    if body_contains_third_party(combined_html):
        return False

    return True


# =========================
# State (dedup)
# =========================
def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"seen": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": {}}


def save_state(path: str, state: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def already_seen(state: dict, rcp_no: str) -> bool:
    return rcp_no in state.get("seen", {})


def mark_seen(state: dict, rcp_no: str, meta: dict) -> None:
    state.setdefault("seen", {})[rcp_no] = meta


# =========================
# Telegram (UI like screenshot)
# =========================
def fmt_msg(item: dict) -> str:
    corp_name = _norm(item.get("corp_name", ""))
    corp_cls  = _norm(item.get("corp_cls", ""))  # Y/K/N
    report_nm = _norm(item.get("report_nm", ""))
    rcp_no    = _norm(item.get("rcp_no", ""))
    rcept_dt  = _norm(item.get("rcept_dt", ""))

    dt_str = rcept_dt
    if len(rcept_dt) == 8:
        dt_str = f"{rcept_dt[:4]}-{rcept_dt[4:6]}-{rcept_dt[6:8]}"

    corp_name_e = html_lib.escape(corp_name)
    report_nm_e = html_lib.escape(report_nm)

    # 스샷처럼 구성(카드형 텍스트)
    text = (
        "📌 <b>증자 공시 감지</b>\n"
        f"• 회사: {corp_name_e} ({corp_cls})\n"
        f"• 접수일: {dt_str}\n"
        "공시\n"
        f"– {report_nm_e}\n"
        f"({rcp_no})"
    )
    return text


def tg_send(text: str, dart_url: str) -> None:
    if not (TG_BOT_TOKEN and TG_CHAT_ID):
        raise RuntimeError("Missing TG_BOT_TOKEN or TG_CHAT_ID env")

    reply_markup = {
        "inline_keyboard": [
            [{"text": "📄 DART 열기", "url": dart_url}]
        ]
    }

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        # 텔레그램은 reply_markup를 JSON 문자열로 보내는 게 가장 안전
        "reply_markup": json.dumps(reply_markup, ensure_ascii=False),
    }

    r = requests.post(TG_SEND_URL, data=payload, timeout=12)
    r.raise_for_status()


# =========================
# DART list fetch
# =========================
def fetch_items_window(days: int) -> list[dict]:
    """
    최근 N일치 공시를 가져오되 corp_cls(Y/K/N)를 각각 조회하여 합침
    """
    end = datetime.now()
    start = end - timedelta(days=max(1, days))

    bgn_de = start.strftime("%Y%m%d")
    end_de = end.strftime("%Y%m%d")

    all_items: dict[str, dict] = {}
    corp_classes = MARKET_CLASSES or ["Y", "K", "N"]

    for cls in corp_classes:
        page_no = 1
        while True:
            params = {
                "crtfc_key": DART_API_KEY,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "page_no": page_no,
                "page_count": 100,
                "corp_cls": cls,
            }
            r = requests.get(DART_LIST_URL, params=params, timeout=15, headers=UA_HEADERS)
            r.raise_for_status()
            data = r.json()

            # 000: 정상, 013: 데이터 없음 등
            if data.get("status") != "000":
                break

            items = data.get("list", []) or []
            for it in items:
                rcp_no = _norm(it.get("rcp_no", ""))
                if rcp_no:
                    all_items[rcp_no] = it

            total_page = int(data.get("total_page", "1") or "1")
            if page_no >= total_page:
                break
            page_no += 1

    def sort_key(x: dict):
        return (_norm(x.get("rcept_dt", "")), _norm(x.get("rcp_no", "")))

    return sorted(all_items.values(), key=sort_key, reverse=True)


# =========================
# Main
# =========================
def validate_env() -> None:
    if not DART_API_KEY:
        raise RuntimeError("Missing DART_API_KEY env")
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise RuntimeError("Missing TG_BOT_TOKEN or TG_CHAT_ID env")


def run_once() -> int:
    validate_env()

    state = load_state(STATE_PATH)
    items = fetch_items_window(LOOKBACK_DAYS)

    scanned = 0
    sent = 0

    for item in items:
        scanned += 1
        report_nm = _norm(item.get("report_nm", ""))
        rcp_no = _norm(item.get("rcp_no", ""))
        if not rcp_no:
            continue

        if already_seen(state, rcp_no):
            continue

        # ✅ 핵심: 유상증자 + 제3자배정 제외
        if not is_allowed_rights_issue(report_nm, rcp_no):
            # 재검사 방지 위해 skipped도 seen 처리
            mark_seen(state, rcp_no, {"skipped": True, "report_nm": report_nm, "ts": int(time.time())})
            continue

        dart_url = DART_VIEW_URL.format(rcpNo=rcp_no)
        try:
            tg_send(fmt_msg(item), dart_url)
            sent += 1
            mark_seen(state, rcp_no, {"sent": True, "report_nm": report_nm, "ts": int(time.time())})
            save_state(STATE_PATH, state)
        except Exception as e:
            print(f"[ERROR] telegram send failed rcp_no={rcp_no}: {e}")
            # 전송 실패면 seen 처리 안 해서 다음에 재시도 가능

    save_state(STATE_PATH, state)
    print(f"[OK] scanned={scanned} unique={len(items)} sent={sent}")
    return sent


def main():
    if POLL_SLEEP_SEC <= 0:
        run_once()
        return

    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[ERROR] run_once: {e}")
        time.sleep(POLL_SLEEP_SEC)


if __name__ == "__main__":
    main()
