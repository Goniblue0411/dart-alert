#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, io, zipfile, html
import requests
from datetime import datetime, timedelta

# =========================
# Secrets (GitHub repo Secrets)
# =========================
DART_API_KEY = os.environ["DART_API_KEY"].strip()
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"].strip()
TG_CHAT_ID   = os.environ["TG_CHAT_ID"].strip()

# =========================
# Config (workflow env override 가능)
# =========================
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))

# 시장 필터 (빈 값이면 전체)
# Y=KOSPI, K=KOSDAQ, N=KONEX, E=OTHER
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "Y,K,N").split(",") if x.strip()]

# 페이지네이션
MAX_PAGES  = int(os.getenv("MAX_PAGES", "12"))
PAGE_COUNT = int(os.getenv("PAGE_COUNT", "100"))

STATE_PATH = "state.json"
SEEN_MAX   = 8000

# =========================
# Filters (정책)
# =========================
# 1) 보고서명: 유상/무상/유무상 "결정"만 대상으로
INC_REPORT = re.compile(
    r"(유상증자\s*결정|유상증자결정|무상증자\s*결정|무상증자결정|유무상증자\s*결정|유무상증자결정)",
    re.I
)

# 2) 원문에서 제3자배정은 무조건 제외
EXC_3RD = re.compile(r"(제\s*3\s*자\s*배정|제3자배정)", re.I)

# 3) 원문에서 허용되는 배정 방식
# - 일반주주배정
ALLOW_GENERAL = re.compile(r"(일반\s*주주\s*배정|일반주주배정)", re.I)
# - 주주배정(구주주 포함)
ALLOW_SHAREHOLDER = re.compile(r"(주주\s*배정|주주배정|구주주\s*청약|구주주청약|구주주)", re.I)

# =========================
# URLs
# =========================
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL  = "https://opendart.fss.or.kr/api/document.xml"
VIEW_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={}"
TG_SEND  = "https://api.telegram.org/bot{}/sendMessage"

S = requests.Session()
S.headers.update({"User-Agent": "dart-alert-github-actions/2.1"})

TG_MAX = 4096

# =========================
# state.json
# =========================
def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        if not isinstance(st, dict):
            return {"seen": []}
        if "seen" not in st or not isinstance(st["seen"], list):
            st["seen"] = []
        return st
    except Exception:
        return {"seen": []}

def save_state(st):
    seen = st.get("seen", [])
    if not isinstance(seen, list):
        seen = []
    st["seen"] = seen[-SEEN_MAX:]
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def is_seen(st, rcept_no: str) -> bool:
    return rcept_no in set(st.get("seen", []))

def mark_seen(st, rcept_no: str):
    st.setdefault("seen", []).append(rcept_no)

# =========================
# Telegram
# =========================
def tg_send(text: str, button_url: str | None = None):
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if button_url:
        payload["reply_markup"] = json.dumps(
            {"inline_keyboard": [[{"text": "📄 DART 열기", "url": button_url}]]},
            ensure_ascii=False
        )

    r = S.post(TG_SEND.format(TG_BOT_TOKEN), data=payload, timeout=30)
    r.raise_for_status()

def tg_send_safe(text: str, button_url: str | None = None):
    if len(text) <= TG_MAX:
        tg_send(text, button_url)
        return
    tg_send(text[: TG_MAX - 40] + "\n\n(…중략)", button_url)

# =========================
# DART list.json (pagination)
# =========================
def market_ok(corp_cls: str) -> bool:
    corp_cls = (corp_cls or "").strip().upper()
    if not MARKET_CLASSES:
        return True
    return corp_cls in MARKET_CLASSES

def fetch_list_pages():
    end_de = datetime.now().strftime("%Y%m%d")
    bgn_de = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    out = []
    for p in range(1, MAX_PAGES + 1):
        j = S.get(
            LIST_URL,
            params=dict(
                crtfc_key=DART_API_KEY,
                bgn_de=bgn_de,
                end_de=end_de,
                pblntf_ty="B",
                page_no=p,
                page_count=PAGE_COUNT,
                sort="date",
                sort_mth="desc",
            ),
            timeout=30,
        ).json()

        status = j.get("status")
        if status == "013":
            break
        if status != "000":
            raise RuntimeError(f"LIST error {status}: {j.get('message')}")

        lst = j.get("list") or []
        if not lst:
            break
        out.extend(lst)

        # total_count 기반 종료 (있을 때만)
        try:
            total = int(j.get("total_count") or 0)
            pc    = int(j.get("page_count") or PAGE_COUNT)
            if total and total <= p * pc:
                break
        except Exception:
            pass

    # rcept_no 중복 제거
    seen = set()
    dedup = []
    for it in out:
        rno = (it.get("rcept_no") or "").strip()
        if not rno or rno in seen:
            continue
        seen.add(rno)
        dedup.append(it)

    # 오래된 것부터 처리
    dedup.sort(key=lambda x: (x.get("rcept_dt", ""), x.get("rcept_no", "")))
    return dedup

# =========================
# document.xml fetch + textify
# =========================
def _xml_to_text(xml_bytes: bytes) -> str:
    s = xml_bytes.decode("utf-8", errors="ignore")

    # 줄바꿈 힌트
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(tr|p|div|li|h\d)>", "\n", s)

    # 태그 제거
    s = re.sub(r"<[^>]+>", " ", s)

    # 엔티티 decode
    s = html.unescape(s)

    # 공백 정리
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n\s+", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)

    return s.strip()

def fetch_document_text(rcept_no: str) -> str:
    r = S.get(DOC_URL, params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=60)
    r.raise_for_status()
    raw = r.content

    texts = []
    # ZIP 판별
    is_zip = (r.headers.get("Content-Type", "").lower().find("zip") >= 0) or (raw[:2] == b"PK")
    if is_zip:
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            for name in z.namelist():
                if name.lower().endswith((".xml", ".html", ".htm")):
                    try:
                        texts.append(_xml_to_text(z.read(name)))
                    except Exception:
                        pass
    else:
        texts.append(_xml_to_text(raw))

    return "\n\n".join([t for t in texts if t])

# =========================
# classify allocation + type
# =========================
def classify_event_type(report_nm: str) -> str:
    rn = report_nm or ""
    if re.search(r"무상증자", rn):
        return "무상"
    if re.search(r"유무상증자", rn):
        return "유무상"
    if re.search(r"유상증자", rn):
        return "유상"
    return "N/A"

def classify_allocation(doc_text: str) -> str:
    """
    return: '일반주주배정' | '주주배정' | 'N/A'
    """
    if ALLOW_GENERAL.search(doc_text):
        return "일반주주배정"
    if ALLOW_SHAREHOLDER.search(doc_text):
        return "주주배정"
    return "N/A"

# =========================
# 청약일정 추출 (원문 기반)
# =========================
def _normalize_ws(s: str) -> str:
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s

def extract_subscription_schedule(doc_text: str) -> str:
    """
    원문 텍스트에서 청약 관련 라인을 여러 개 수집해서 한 줄로 합침.
    - 우리사주/구주주/일반공모 등 라벨이 있으면 그 라벨 우선
    - 없으면 '청약일' 라인이라도 수집
    """
    if not doc_text:
        return "N/A"

    # 라벨 후보 (우선순위)
    labels = [
        "우리사주조합 청약일", "우리사주조합청약일",
        "구주주 청약일", "구주주청약일",
        "일반공모 청약일", "일반공모청약일",
        "일반청약일", "청약일",
    ]

    lines = [ln.strip() for ln in doc_text.splitlines() if ln.strip()]
    hits = []

    # 1) 라벨 기반 수집
    for ln in lines:
        for lb in labels:
            if lb in ln:
                after = ln.split(lb, 1)[1]
                after = re.sub(r"^[\s:\-·•\)]+", "", after).strip()
                after = _normalize_ws(after)

                # 값이 비면 다음 라인 붙이기(표 분리 대비)
                if len(after) < 2:
                    # 다음 줄 1개만 붙이기(과도 결합 방지)
                    idx = None
                    try:
                        idx = lines.index(ln)
                    except Exception:
                        idx = None
                    if idx is not None and idx + 1 < len(lines):
                        nxt = _normalize_ws(lines[idx + 1])
                        if nxt and lb not in nxt:
                            after = (after + " " + nxt).strip()

                if after:
                    hits.append(f"{lb}: {after}")

    # 2) 라벨이 전혀 없으면, '청약' 키워드 라인 중 날짜 패턴이 있는 것 수집
    if not hits:
        date_pat = re.compile(
            r"(\d{4}\s*[.\-/년]\s*\d{1,2}\s*[.\-/월]\s*\d{1,2}\s*일?)"
            r"(\s*[~\-]\s*)?"
            r"(\d{4}\s*[.\-/년]\s*\d{1,2}\s*[.\-/월]\s*\d{1,2}\s*일?)?",
            re.I
        )
        for ln in lines:
            if "청약" in ln:
                m = date_pat.search(ln)
                if m:
                    hits.append(_normalize_ws(ln))

    # 중복 제거(순서 유지)
    uniq = []
    seen = set()
    for h in hits:
        key = re.sub(r"\s+", " ", h)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(h)

    if not uniq:
        return "N/A"

    # 너무 길면 적당히 자르기
    out = " / ".join(uniq)
    if len(out) > 350:
        out = out[:350] + "…"
    return out

# =========================
# main
# =========================
def main():
    st = load_state()
    sent = 0

    items = fetch_list_pages()

    for it in items:
        rno = (it.get("rcept_no") or "").strip()
        if not rno:
            continue
        if is_seen(st, rno):
            continue

        corp_cls = (it.get("corp_cls") or "").strip().upper()
        if not market_ok(corp_cls):
            mark_seen(st, rno)
            continue

        rpt_nm = (it.get("report_nm") or "").strip()
        if not INC_REPORT.search(rpt_nm):
            mark_seen(st, rno)
            continue

        # 원문 텍스트 가져와서 배정방식/제3자 여부 판정
        try:
            doc_text = fetch_document_text(rno)
        except Exception:
            # 원문을 못 가져오면 정책상 안전하게 제외(원문 기반 필터라서)
            mark_seen(st, rno)
            continue

        # 제3자배정 포함이면 무조건 제외
        if EXC_3RD.search(doc_text):
            mark_seen(st, rno)
            continue

        alloc = classify_allocation(doc_text)
        if alloc == "N/A":
            # 일반주주/주주배정이 아닌 케이스는 제외
            mark_seen(st, rno)
            continue

        # ✅ 청약일정 추출
        subs = extract_subscription_schedule(doc_text)

        ev_type = classify_event_type(rpt_nm)

        corp = (it.get("corp_name") or "N/A").strip()
        rcept_dt = (it.get("rcept_dt") or "").strip()
        url = VIEW_URL.format(rno)
        market_name = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "OTHER"}.get(corp_cls, corp_cls or "N/A")

        lines = [
            "📌 <b>증자 공시 감지</b>",
            f"• 회사: <b>{html.escape(corp)}</b> <i>({html.escape(market_name)})</i>",
            f"• 유형: <b>{html.escape(ev_type)}</b> / 배정: <b>{html.escape(alloc)}</b>",
            f"• 접수일: {html.escape(rcept_dt or 'N/A')}",
            f"• 공시명: {html.escape(rpt_nm)}",
            f"• 청약일정: {html.escape(subs)}",
        ]

        tg_send_safe("\n".join(lines), button_url=url)
        mark_seen(st, rno)
        sent += 1

    save_state(st)
    print(f"OK sent={sent} seen={len(st.get('seen', []))}")

if __name__ == "__main__":
    main()
