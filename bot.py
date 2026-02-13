#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DART 증자 공시 텔레그램 알림 (완성형 통합본)
- 유상/무상 증자 공시 감지
- ✅ 제3자배정(제3자배정증자 포함) "절대" 제외 (제목/HTML/원문 document.xml zip 다 검사)
- 텔레그램 카드 + "📄 DART 열기" 버튼 유지
- GitHub Actions(1회 실행) / VPS 상시루프(POLL_SLEEP_SEC>0) 둘 다 지원
"""

import os
import json
import time
import re
import html
import io
import zipfile
import requests
from datetime import datetime, timedelta

# =========================
# ENV
# =========================
DART_API_KEY = os.getenv("4e34368459edf9be284521643b0b623f94684efe", "").strip()
TG_BOT_TOKEN = os.getenv("8337357668:AAHy1zroWzyuBzm95FNOWq_pXcaPb0sepv8", "").strip()
TG_CHAT_ID   = os.getenv("8398762332", "").strip()

LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "3"))
MARKET_CLASSES  = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "Y,K,N").split(",") if x.strip()]
POLL_SLEEP_SEC  = int(os.getenv("POLL_SLEEP_SEC", "0"))  # GitHub Actions는 0 권장(한번만 실행)

# ✅ 원문(document.xml) 읽기 실패 시 정책
# - "strict": 원문 못 읽으면 안전하게 제외(=알림 안 보냄)  -> 제3자배정 절대 새지 않음
# - "lenient": 원문 못 읽으면 HTML/제목만으로 판단 후 통과 가능 -> 누락은 줄지만, 제3자배정이 새는 위험 존재
DOC_FAIL_POLICY = os.getenv("DOC_FAIL_POLICY", "strict").strip().lower()

STATE_PATH = "state.json"

DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_DOC_URL  = "https://opendart.fss.or.kr/api/document.xml"  # zip 반환
DART_VIEW_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcpNo}"

TG_SEND_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"

S = requests.Session()
S.headers.update({"User-Agent": "dart-alert/ops/2.0"})

# =========================
# Regex / Keywords
# =========================
# 기본: 증자 관련
INC_TITLE = re.compile(r"(유상증자|무상증자)", re.I)

# 유상/무상 결정/정정류 (넓게)
INC_REPORT = re.compile(
    r"(유상증자결정|무상증자결정|주요사항보고서\(유상증자결정\)|주요사항보고서\(무상증자결정\)|정정.*유상증자|정정.*무상증자|유상증자또는주식관련사채등의발행결과)",
    re.I
)

# ✅ 제3자배정 제외(강화: 제삼자/띄어쓰기/영문/증자 단어 포함 변형까지)
THIRD_PARTY = re.compile(
    r"(제\s*[삼3]\s*자\s*배정(\s*증자)?|제\s*[삼3]\s*자\s*배정\s*유상증자|third\s*party|3rd\s*party)",
    re.I
)

# 포함하고 싶은 “일반/주주배정” 힌트(문서에서 가점)
INCLUDE_HINT = re.compile(r"(일반공모|일반\s*주주|주주배정|구주주|기존주주)", re.I)

# HTML/XML 태그 제거용(대충 텍스트화)
TAG_RE = re.compile(r"<[^>]+>")

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
    r = S.get(DART_LIST_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def get_view_html(rcp_no: str) -> str:
    """
    DART 뷰어(main.do) HTML
    - 텍스트가 스크립트로 로딩되는 경우가 많아 '보조' 자료로만 사용
    """
    url = DART_VIEW_URL.format(rcpNo=rcp_no)
    r = S.get(url, timeout=25)
    if not r.ok:
        return ""
    return r.text

def _decode_best_effort(raw: bytes) -> str:
    if not raw:
        return ""
    # utf-8 우선, 실패하면 euc-kr/latin-1 순으로 시도
    for enc in ("utf-8", "euc-kr", "cp949", "latin-1"):
        try:
            s = raw.decode(enc, errors="ignore")
            if s and s.strip():
                return s
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")

def get_document_text(rcp_no: str) -> str:
    """
    ✅ OpenDART document.xml (zip)에서 원문 텍스트 추출
    - 실패하면 "" 반환
    """
    try:
        params = {"crtfc_key": DART_API_KEY, "rcept_no": rcp_no}
        r = S.get(DART_DOC_URL, params=params, timeout=25)
        if not r.ok or not r.content:
            return ""

        content = r.content

        # zip이면 PK로 시작
        if content.startswith(b"PK"):
            zf = zipfile.ZipFile(io.BytesIO(content))
            texts = []
            for name in zf.namelist():
                ln = name.lower()
                if not (ln.endswith(".xml") or ln.endswith(".html") or ln.endswith(".htm")):
                    continue
                try:
                    raw = zf.read(name)
                except Exception:
                    continue

                s = _decode_best_effort(raw)
                if not s.strip():
                    continue

                # 태그 제거 + HTML 엔티티 해제
                s = TAG_RE.sub(" ", s)
                s = html.unescape(s)
                texts.append(s)

            # 너무 길어지면 가드
            return "\n".join(texts)[:2_000_000]

        # zip이 아니면 그냥 텍스트/XML일 수 있음
        return _decode_best_effort(content)

    except Exception:
        return ""

def market_cls_from_report(item) -> str:
    """
    list.json에는 corp_cls가 있을 때도 있고 없을 때도 있음(상황에 따라).
    """
    return (item.get("corp_cls") or "").strip().upper()

def should_consider(item) -> bool:
    """
    1차 제목 기반 필터: 증자 관련 문서만
    """
    report_nm = (item.get("report_nm") or "").strip()
    if not report_nm:
        return False

    if not INC_TITLE.search(report_nm):
        return False

    # 너무 빡빡하면 누락될 수 있어 넓게 통과
    if not INC_REPORT.search(report_nm):
        return True

    return True

def is_third_party_strict(rcp_no: str, report_nm: str, html_text: str, doc_text: str) -> bool:
    """
    ✅ 제3자배정 '절대 제외' 정책:
    - 1) 제목에 제3자배정 변형이 있으면 즉시 제외
    - 2) 원문(document.xml)에서 발견되면 제외 (가장 확실)
    - 3) HTML(보조)에서 발견되면 제외
    - 4) 원문을 못 읽으면 정책(DOC_FAIL_POLICY)에 따라:
       - strict: 안전하게 제외(알림 안 보냄)
       - lenient: 제목/HTML만으로 판단
    """
    # 1) 제목
    if THIRD_PARTY.search(report_nm or ""):
        return True

    # 2) 원문
    if doc_text:
        if THIRD_PARTY.search(doc_text):
            return True
        return False

    # 3) 원문이 없으면 HTML 보조 검사
    if html_text and THIRD_PARTY.search(html_text):
        return True

    # 4) 최종 정책
    if DOC_FAIL_POLICY == "strict":
        return True

    return False

def is_in_scope(report_nm: str, scope_text: str) -> bool:
    """
    - 무상증자: 제3자배정만 아니면 통과
    - 유상증자: 제3자배정 제외, 그 외는 통과(누락 방지)
      * "일반/주주배정만"으로 더 강하게 제한하고 싶으면 마지막 return True를 False로 바꾸면 됨.
    """
    if not scope_text:
        return False

    if re.search(r"무상증자", report_nm or "", re.I):
        return True

    if INCLUDE_HINT.search(scope_text):
        return True

    # 힌트가 없어도(표/문서 구조 차이) 제3자만 아니면 일단 통과
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

            # 보조 HTML
            html_text = get_view_html(rcp_no)

            # ✅ 원문 텍스트(가장 확실)
            doc_text = get_document_text(rcp_no)

            # ✅ 제3자배정 "절대 제외"
            if is_third_party_strict(rcp_no, report_nm, html_text, doc_text):
                seen.add(rcp_no)
                continue

            # 범위 판단은 원문 텍스트 우선
            scope_text = doc_text if doc_text else html_text

            if not is_in_scope(report_nm, scope_text):
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
    new_hits.sort(key=lambda x: (x.get("rcept_dt", ""), x.get("rcept_no", "")))

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
