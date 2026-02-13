#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import re
import html
import requests
from datetime import datetime, timedelta

# =========================
# ENV
# =========================
DART_API_KEY = os.getenv("4e34368459edf9be284521643b0b623f94684efe", "").strip()
TG_BOT_TOKEN = os.getenv("8337357668:AAHDQJcYB3VWvo15uP6Q9uZSLn40Q2MCjE​", "").strip()
TG_CHAT_ID   = os.getenv("8398762332", "").strip()

LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "3"))
MARKET_CLASSES  = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "Y,K,N").split(",") if x.strip()]
POLL_SLEEP_SEC  = int(os.getenv("POLL_SLEEP_SEC", "0"))  # GitHub Actions는 0 권장(한번만 실행)

STATE_PATH = "state.json"

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_VIEW_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcpNo}"

TG_SEND_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

S = requests.Session()
S.headers.update({"User-Agent": "dart-alert/ops/1.0"})

# =========================
# Regex / Keywords
# =========================
# 기본: 증자 관련
INC_TITLE = re.compile(r"(유상증자|무상증자)", re.I)
# 유상/무상 결정/정정류 (넓게)
INC_REPORT = re.compile(r"(유상증자결정|무상증자결정|주요사항보고서\(유상증자결정\)|주요사항보고서\(무상증자결정\)|정정.*유상증자|정정.*무상증자)", re.I)

# 제3자배정 제외(본문에서 확정)
THIRD_PARTY = re.compile(r"(제\s*3\s*자\s*배정|제3자배정)", re.I)

# 포함하고 싶은 “일반/주주배정” 힌트(본문에서 가점)
INCLUDE_HINT = re.compile(r"(일반공모|일반\s*주주|주주배정|구주주|기존주주)", re.I)

# =========================
# Helpers
# =========================
def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        if "seen" not in st or not isinstance(st["seen"], list):
            st["seen"] = []
        return st
    except Exception:
        return {"seen": []}

def save_state(st):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def tg_send_card(title: str, body_lines: list, button_url: str):
    """
    HTML 모드로 안전 전송 + 항상 버튼 포함
    """
    safe_title = html.escape(title)
    safe_lines = [html.escape(x) for x in body_lines if x and x.strip()]
    text = f"<b>{safe_title}</b>\n" + "\n".join(safe_lines)

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": json.dumps({
            "inline_keyboard": [
                [{"text": "📄 DART 열기", "url": button_url}]
            ]
        }, ensure_ascii=False)
    }

    r = S.post(TG_SEND_URL, data=payload, timeout=20)
    if not r.ok:
        raise RuntimeError(f"Telegram send failed: {r.status_code} {r.text[:200]}")

def dart_list(start_date: str, end_date: str, page_no: int):
    """
    DART list.json pagination
    """
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": start_date,
        "end_de": end_date,
        "page_no": page_no,
        "page_count": 100,
    }
    # market classes filter: corp_cls can be only one in API; 그래서 여러개면 반복 호출보다
    # 여기서는 전체 받아서 후필터(안정성 위해).
    r = S.get(DART_LIST_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def get_view_html(rcp_no: str) -> str:
    url = DART_VIEW_URL.format(rcpNo=rcp_no)
    r = S.get(url, timeout=25)
    # 200이 아니어도 필터링이 중요하니 예외 대신 빈 문자열 처리
    if not r.ok:
        return ""
    return r.text

def market_cls_from_report(item) -> str:
    """
    list.json에는 corp_cls가 있을 때도 있고 없을 때도 있음(상황에 따라).
    있으면 사용하고, 없으면 빈값.
    """
    v = (item.get("corp_cls") or "").strip().upper()
    return v

def should_consider(item) -> bool:
    """
    1차 제목 기반 필터: 증자 관련 문서만
    """
    report_nm = (item.get("report_nm") or "").strip()
    if not report_nm:
        return False
    if not INC_TITLE.search(report_nm):
        return False
    if not INC_REPORT.search(report_nm):
        # 너무 빡빡하면 누락될 수 있어 넓게 통과시키고 후단 HTML에서 판단 가능
        return True
    return True

def is_third_party_by_html(html_text: str) -> bool:
    if not html_text:
        # HTML을 못가져오면 “안전하게” 제외할지/포함할지 선택 필요.
        # 너 요구는 "제3자배정은 절대 안 나오게" -> HTML 실패 시 보수적으로 제외.
        return True
    return bool(THIRD_PARTY.search(html_text))

def is_in_scope_by_html(html_text: str, report_nm: str) -> bool:
    """
    - 무상증자: 보통 제3자배정 이슈 없음 -> HTML 제3자만 아니면 통과
    - 유상증자: 제3자배정 제외, 그리고 일반/주주배정 힌트가 없으면 애매하지만
      제목이 유상증자결정이면 통과시키되, 제3자만 확실히 제외.
    """
    if not html_text:
        return False

    # 제3자배정이면 무조건 제외
    if THIRD_PARTY.search(html_text):
        return False

    # 무상은 통과
    if re.search(r"무상증자", report_nm, re.I):
        return True

    # 유상은: “일반/주주배정” 힌트가 있으면 확실히 통과
    if INCLUDE_HINT.search(html_text):
        return True

    # 힌트가 없어도, 제3자만 아니면 일단 통과(너가 일반/주주배정만 원하지만
    # 문서 구조상 힌트가 누락되는 경우가 있어 누락 방지용)
    # 더 강하게 제한하고 싶으면 아래 줄을 False로 바꾸면 됨.
    return True

def fmt_date_yyyymmdd_to_iso(s: str) -> str:
    # rcept_dt: "20260211"
    if not s or len(s) != 8:
        return s
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"

def main_once():
    if not DART_API_KEY:
        raise RuntimeError("DART_API_KEY is missing")
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        raise RuntimeError("TG_BOT_TOKEN or TG_CHAT_ID is missing")

    st = load_state()
    seen = set(st.get("seen", []))

    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    bgn_de = start_dt.strftime("%Y%m%d")
    end_de = end_dt.strftime("%Y%m%d")

    new_hits = []

    page_no = 1
    total_pages = 1

    while page_no <= total_pages:
        data = dart_list(bgn_de, end_de, page_no)

        if str(data.get("status")) != "000":
            # DART 오류면 바로 중단(재시도는 Actions가 해줌)
            raise RuntimeError(f"DART list error: {data.get('status')} / {data.get('message')}")

        total_count = int(data.get("total_count") or 0)
        page_count = int(data.get("page_count") or 100)
        total_pages = (total_count + page_count - 1) // page_count if total_count > 0 else 1

        for item in data.get("list", []) or []:
            rcp_no = (item.get("rcept_no") or "").strip()
            if not rcp_no or rcp_no in seen:
                continue

            # 시장 필터 (가능하면 corp_cls 이용)
            corp_cls = market_cls_from_report(item)
            if MARKET_CLASSES and corp_cls and (corp_cls not in MARKET_CLASSES):
                continue

            if not should_consider(item):
                continue

            report_nm = (item.get("report_nm") or "").strip()
            corp_name = (item.get("corp_name") or "").strip()
            rcept_dt  = fmt_date_yyyymmdd_to_iso((item.get("rcept_dt") or "").strip())

            view_url = DART_VIEW_URL.format(rcpNo=rcp_no)

            # HTML로 제3자배정 확정 필터
            html_text = get_view_html(rcp_no)

            # HTML을 못받으면 “제3자배정 절대 제외” 정책상 제외
            if is_third_party_by_html(html_text):
                seen.add(rcp_no)
                continue

            # 범위(일반/주주배정 + 무상/유상) 통과 판단
            if not is_in_scope_by_html(html_text, report_nm):
                seen.add(rcp_no)
                continue

            new_hits.append({
                "rcept_no": rcp_no,
                "corp_name": corp_name,
                "corp_cls": corp_cls,
                "rcept_dt": rcept_dt,
                "report_nm": report_nm,
                "view_url": view_url
            })

            seen.add(rcp_no)

        page_no += 1

    # 최신순 정렬(받는쪽 보기 좋게)
    new_hits.sort(key=lambda x: (x.get("rcept_dt",""), x.get("rcept_no","")))

    # 전송
    for h in new_hits:
        corp_cls = h["corp_cls"] or ""
        suffix = f" ({corp_cls})" if corp_cls else ""
        title = "📌 증자 공시 감지"
        body = [
            f"• 회사: {h['corp_name']}{suffix}",
            f"• 접수일: {h['rcept_dt']}",
            "",
            "공시",
            f"– {h['report_nm']}",
            f"({h['rcept_no']})"
        ]
        try:
            tg_send_card(title, body, h["view_url"])
        except Exception as e:
            # 텔레그램 전송 실패해도 state는 저장해야 중복폭탄 방지
            # 에러는 콘솔에 남김(GitHub Actions 로그)
            print(f"[TG ERROR] {h['rcept_no']} {e}")

    # state 저장
    st["seen"] = list(seen)[-5000:]  # 너무 커지는거 방지
    save_state(st)

    print(f"OK sent={len(new_hits)} seen={len(st['seen'])}")

def main():
    # GitHub Actions: 보통 1회 실행 후 종료
    main_once()

    # VPS 상시 루프 모드 필요하면 아래 활성화
    if POLL_SLEEP_SEC > 0:
        while True:
            time.sleep(POLL_SLEEP_SEC)
            try:
                main_once()
            except Exception as e:
                print(f"[LOOP ERROR] {e}")

if __name__ == "__main__":
    main()
