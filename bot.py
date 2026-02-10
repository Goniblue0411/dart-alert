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
# Config
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
ALLOW_GENERAL = re.compile(r"(일반\s*주주\s*배정|일반주주배정)", re.I)
ALLOW_SHAREHOLDER = re.compile(r"(주주\s*배정|주주배정|구주주\s*청약|구주주청약|구주주)", re.I)

# =========================
# URLs
# =========================
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DOC_URL  = "https://opendart.fss.or.kr/api/document.xml"
VIEW_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={}"
TG_SEND  = "https://api.telegram.org/bot{}/sendMessage"

S = requests.Session()
S.headers.update({"User-Agent": "dart-alert-github-actions/4.0"})

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

        # total_count 기반 종료(있을 때만)
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
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(tr|p|div|li|h\d)>", "\n", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n\s+", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def fetch_document_text(rcept_no: str) -> str:
    r = S.get(DOC_URL, params={"crtfc_key": DART_API_KEY, "rcept_no": rcept_no}, timeout=60)
    r.raise_for_status()
    raw = r.content

    texts = []
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
    if ALLOW_GENERAL.search(doc_text):
        return "일반주주배정"
    if ALLOW_SHAREHOLDER.search(doc_text):
        return "주주배정"
    return "N/A"

# =========================
# Field extraction
# =========================
def _norm_ws(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "")).strip()

def pick_first_by_labels(text: str, labels: list[str], maxlen: int = 140) -> str:
    if not text:
        return "N/A"
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        for lb in labels:
            if lb in ln:
                after = ln.split(lb, 1)[1]
                after = re.sub(r"^[\s:\-·•\)]+", "", after).strip()
                after = _norm_ws(after)

                if len(after) < 2 and i + 1 < len(lines):
                    nxt = _norm_ws(lines[i + 1])
                    if nxt and not any(x in nxt for x in labels):
                        after = _norm_ws((after + " " + nxt).strip())

                if after:
                    return after[:maxlen].strip()
    return "N/A"

def pick_multi_by_labels(text: str, labels: list[str], max_items: int = 6, maxlen_each: int = 90) -> str:
    if not text:
        return "N/A"
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    hits = []
    for i, ln in enumerate(lines):
        for lb in labels:
            if lb in ln:
                after = ln.split(lb, 1)[1]
                after = re.sub(r"^[\s:\-·•\)]+", "", after).strip()
                after = _norm_ws(after)
                if len(after) < 2 and i + 1 < len(lines):
                    nxt = _norm_ws(lines[i + 1])
                    if nxt and lb not in nxt:
                        after = _norm_ws((after + " " + nxt).strip())
                if after:
                    hits.append(f"{lb}: {after[:maxlen_each].strip()}")

    uniq, seen = [], set()
    for h in hits:
        key = re.sub(r"\s+", " ", h)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(h)
        if len(uniq) >= max_items:
            break

    if not uniq:
        return "N/A"
    out = " / ".join(uniq)
    return out[:420] + ("…" if len(out) > 420 else "")

def extract_money_purpose(text: str) -> str:
    v = pick_first_by_labels(text, [
        "자금조달의 목적", "자금조달 목적", "자금조달의목적",
        "자금의 사용목적", "자금사용목적", "자금 사용 목적",
        "조달자금의 사용목적", "조달 자금의 사용목적",
    ], maxlen=220)
    if v != "N/A":
        return v

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    keys = ["시설", "운영", "채무", "타법인", "기타", "연구", "M&A", "인수", "투자"]
    amt_pat = re.compile(r"(\d{1,3}(?:,\d{3})+|\d+)\s*원")
    hits = []
    for ln in lines:
        if any(k in ln for k in keys) and amt_pat.search(ln):
            hits.append(_norm_ws(ln))
        if len(hits) >= 4:
            break
    if hits:
        out = " / ".join(hits)
        return out[:240] + ("…" if len(out) > 240 else "")
    return "N/A"

def extract_fields(doc_text: str) -> dict:
    fields = {}
    fields["자금조달의목적"] = extract_money_purpose(doc_text)

    fields["신주배정기준일"] = pick_first_by_labels(doc_text, [
        "신주배정기준일", "신주 배정 기준일", "배정기준일", "배정 기준일",
        "신주배정 기준일", "권리락 기준일",
    ])

    fields["예정가"] = pick_first_by_labels(doc_text, [
        "예정발행가액", "예정 발행가액", "발행가액(예정)", "발행가액 (예정)",
        "예정발행가", "예정 발행가", "예정가", "예정가액",
        "1주당 발행가액(예정)", "1주당 발행가액 (예정)",
    ], maxlen=160)

    fields["확정일"] = pick_first_by_labels(doc_text, [
        "발행가액확정일", "발행가액 확정일",
        "확정일", "가격확정일", "가격 확정일",
        "발행가 확정일", "발행가액의 확정일",
    ])

    fields["신주인수권상장예정기간"] = pick_first_by_labels(doc_text, [
        "신주인수권증서 상장예정기간", "신주인수권증서상장예정기간",
        "신주인수권 상장예정기간", "신주인수권상장예정기간",
        "신주인수권증서 상장기간", "신주인수권증서상장기간",
        "신주인수권 상장기간", "신주인수권상장기간",
    ], maxlen=200)

    fields["청약일"] = pick_multi_by_labels(doc_text, [
        "우리사주조합 청약일", "우리사주조합청약일",
        "구주주 청약일", "구주주청약일",
        "일반공모 청약일", "일반공모청약일",
        "일반청약일",
        "청약일",
    ])

    fields["신주의상장예정일"] = pick_first_by_labels(doc_text, [
        "신주의 상장예정일", "신주의상장예정일",
        "신주 상장예정일", "신주상장예정일",
        "신주권 상장예정일", "신주권상장예정일",
        "상장예정일",
    ])

    return fields

# =========================
# N/A 숨김 + 카드 렌더링 + 위험도
# =========================
def _is_empty_value(v: str) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if not s:
        return True
    if s.upper() == "N/A":
        return True
    if s in ("0", "0원", "0 주", "0주"):
        return True
    return False

def add_if(lines: list[str], label: str, value: str):
    if _is_empty_value(value):
        return
    lines.append(f"• <b>{html.escape(label)}</b>: {html.escape(value)}")

def risk_score(ev_type: str, alloc: str, market: str, doc_text: str, fields: dict) -> tuple[int, str, str]:
    """
    0~100 휴리스틱.
    - 유상/유무상 > 무상
    - KOSDAQ/KONEX 가중
    - 채무/운영 목적 가중
    - 청약/예정가/확정일/인수권기간/상장예정일 정보가 많을수록(=유상 성격) 가중
    """
    score = 10

    et = (ev_type or "").strip()
    if et == "유상":
        score += 40
    elif et == "유무상":
        score += 30
    elif et == "무상":
        score += 10
    else:
        score += 15

    mk = (market or "").upper()
    if mk == "KOSDAQ":
        score += 10
    elif mk == "KONEX":
        score += 15
    elif mk == "KOSPI":
        score += 5
    else:
        score += 7

    # 배정 방식
    if alloc == "주주배정":
        score += 8
    elif alloc == "일반주주배정":
        score += 6

    # 목적 키워드
    purpose = (fields.get("자금조달의목적") or "")
    if re.search(r"(채무|상환|차입|대출)", purpose):
        score += 18
    if re.search(r"(운영|운전자금)", purpose):
        score += 10
    if re.search(r"(타법인|M&A|인수|취득|투자)", purpose):
        score += 12

    # 일정/가격 정보가 많이 잡히면 실제 청약/발행 프로세스 가능성이 높음
    for k in ["청약일", "예정가", "확정일", "신주인수권상장예정기간", "신주의상장예정일"]:
        if not _is_empty_value(fields.get(k, "N/A")):
            score += 4

    # 원문에서 "할인" "보통주" 등도 약간 반영(가벼운 힌트)
    if re.search(r"(할인|발행가액|인수권)", doc_text):
        score += 4

    # clamp
    score = max(0, min(100, score))

    if score >= 75:
        emoji, grade = "🔴", "높음"
    elif score >= 55:
        emoji, grade = "🟠", "중간"
    elif score >= 35:
        emoji, grade = "🟡", "낮음"
    else:
        emoji, grade = "🟢", "매우낮음"
    return score, grade, emoji

def build_card(corp: str, market: str, ev_type: str, alloc: str, rcept_dt: str, rpt_nm: str, url: str,
               doc_text: str, fields: dict) -> str:
    score, grade, emoji = risk_score(ev_type, alloc, market, doc_text, fields)

    # 카드 헤더
    lines = []
    lines.append(f"{emoji} <b>증자 공시 감지</b>  <i>(위험도 {score}/100 · {grade})</i>")
    lines.append(f"🏢 <b>{html.escape(corp)}</b>  <i>({html.escape(market)})</i>")
    lines.append(f"🧾 유형: <b>{html.escape(ev_type)}</b> / 배정: <b>{html.escape(alloc)}</b>")
    if rcept_dt:
        lines.append(f"📅 접수일: {html.escape(rcept_dt)}")
    lines.append("────────────────────")
    lines.append(f"📌 <b>공시명</b>")
    lines.append(f"{html.escape(rpt_nm)}")
    lines.append("────────────────────")

    # 핵심 요약(필드 중 값 있는 것만 노출)
    core = []
    add_if(core, "자금조달의목적", fields.get("자금조달의목적", "N/A"))
    add_if(core, "신주배정기준일", fields.get("신주배정기준일", "N/A"))
    if core:
        lines.append("🧠 <b>핵심</b>")
        lines.extend(core)
        lines.append("────────────────────")

    # 가격/일정 섹션
    price = []
    add_if(price, "예정가", fields.get("예정가", "N/A"))
    add_if(price, "확정일", fields.get("확정일", "N/A"))
    if price:
        lines.append("💰 <b>가격</b>")
        lines.extend(price)
        lines.append("────────────────────")

    sched = []
    add_if(sched, "신주인수권상장예정기간", fields.get("신주인수권상장예정기간", "N/A"))
    add_if(sched, "청약일", fields.get("청약일", "N/A"))
    add_if(sched, "신주의상장예정일", fields.get("신주의상장예정일", "N/A"))
    if sched:
        lines.append("🗓️ <b>일정</b>")
        lines.extend(sched)
        lines.append("────────────────────")

    # 맨 아래 링크 문구(버튼이 있으니 텍스트는 짧게)
    lines.append("➡️ 아래 버튼으로 원문 확인")

    return "\n".join(lines)

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

        # 원문 텍스트 가져오기
        try:
            doc_text = fetch_document_text(rno)
        except Exception:
            mark_seen(st, rno)
            continue

        # 제3자배정 포함이면 제외
        if EXC_3RD.search(doc_text):
            mark_seen(st, rno)
            continue

        alloc = classify_allocation(doc_text)
        if alloc == "N/A":
            mark_seen(st, rno)
            continue

        fields = extract_fields(doc_text)
        ev_type = classify_event_type(rpt_nm)

        corp = (it.get("corp_name") or "N/A").strip()
        rcept_dt = (it.get("rcept_dt") or "").strip()
        url = VIEW_URL.format(rno)
        market_name = {"Y": "KOSPI", "K": "KOSDAQ", "N": "KONEX", "E": "OTHER"}.get(corp_cls, corp_cls or "N/A")

        msg = build_card(
            corp=corp,
            market=market_name,
            ev_type=ev_type,
            alloc=alloc,
            rcept_dt=rcept_dt,
            rpt_nm=rpt_nm,
            url=url,
            doc_text=doc_text,
            fields=fields
        )

        tg_send_safe(msg, button_url=url)

        mark_seen(st, rno)
        sent += 1

    save_state(st)
    print(f"OK sent={sent} seen={len(st.get('seen', []))}")

if __name__ == "__main__":
    main()
