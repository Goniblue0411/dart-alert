#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
dart-alert bot.py (serverless via GitHub Actions-friendly)

- Pagination (page_no loop) to avoid missing items
- Better regex matching + exclude 3rd-party allotment (제3자배정) robustly
- Group consecutive disclosures per company into 1 Telegram "card"
- HTML format + hide N/A fields
- Inline keyboard button(s): "📄 DART 열기"
- Extract extra fields:
  - financing purpose (자금사용목적)
  - record date (신주배정기준일)
  - expected price (예정가), confirm date (확정일)
  - rights listing period (신주인수권상장예정기간)
  - subscription period (청약일)
  - listing date (신주의상장예정일)
- Optional risk score:
  - raise_amount / market_cap ratio
  - discount (offer price vs current price)
  - (best-effort) maximum shareholder participation (text-based hint)
"""

import os
import re
import json
import time
import html
import requests
from datetime import datetime, timedelta

# =========================
# ENV
# =========================
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()
TG_BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.environ.get("TG_CHAT_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
# K:KOSPI, Q:KOSDAQ, N:KONEX (you used Y,K,N earlier in workflow; keep compatible)
# We'll accept both "Y" (KOSPI) and "K" (KOSDAQ) if user had legacy.
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "Y,K,N").split(",") if x.strip()]
MIN_RAISE_KRW = int(os.getenv("MIN_RAISE_KRW", "0"))  # raise amount filter (KRW)

STATE_PATH = os.getenv("STATE_PATH", "state.json")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

# DART endpoints
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
PIFRIC_URL = "https://opendart.fss.or.kr/api/pifricDecsn.json"     # 유상증자결정(주요사항)
COMPANY_URL = "https://opendart.fss.or.kr/api/company.json"        # corp_code -> stock_code
DART_VIEW = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

TG_SEND = "https://api.telegram.org/bot{token}/sendMessage"

# Naver Finance (best-effort) for current price / market cap
NAVER_ITEM = "https://finance.naver.com/item/main.nhn?code={stock_code}"

# =========================
# Matching rules
# =========================
# Include: 일반공모/주주배정 유상/무상/유무상 관련 공시 (정정 포함)
# We'll match report_nm + also validate by body-text when needed.
INC_RE = re.compile(
    r"(무상증자결정|유상증자결정|유무상증자결정|"
    r"주요사항보고서\((무상증자결정|유상증자결정|유무상증자결정)\)|"
    r"\[기재정정\]\s*주요사항보고서\((무상증자결정|유상증자결정|유무상증자결정)\))",
    re.I
)

# Exclude: 제3자배정
EXC_RE_TITLE = re.compile(r"제\s*3\s*자\s*배정|제3자배정", re.I)
# Also exclude if body contains it (stronger)
EXC_RE_BODY = re.compile(r"제\s*3\s*자\s*배정|제3자배정", re.I)

# Prefer include allocation types in body (shareholder/general)
# (If not found, still allow if not 3rd-party; some reports omit keywords)
PREFER_INC_BODY = re.compile(r"(주주\s*배정|일반\s*공모|일반\s*주주|구주주\s*청약)", re.I)

# =========================
# Helpers
# =========================
S = requests.Session()
S.headers.update({"User-Agent": "dart-alert-actions/2.0"})

def must_env():
    missing = []
    if not DART_API_KEY: missing.append("DART_API_KEY")
    if not TG_BOT_TOKEN: missing.append("TG_BOT_TOKEN")
    if not TG_CHAT_ID:   missing.append("TG_CHAT_ID")
    if missing:
        raise SystemExit(f"[ERROR] Missing env: {', '.join(missing)}")

def load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": []}

def save_state(st):
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def seen_has(st, rcept_no: str) -> bool:
    return rcept_no in set(st.get("seen", []))

def seen_add(st, rcept_no: str):
    st.setdefault("seen", [])
    if rcept_no not in st["seen"]:
        st["seen"].append(rcept_no)
    # keep size bounded
    if len(st["seen"]) > 5000:
        st["seen"] = st["seen"][-4000:]

def tg_send_html(text_html: str, buttons=None):
    """
    buttons: list of dicts like [{"text":"📄 DART 열기","url":"https://..."}]
    """
    url = TG_SEND.format(token=TG_BOT_TOKEN)
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if buttons:
        # Telegram inline keyboard
        # Put each button in its own row (clean on mobile)
        keyboard = [[{"text": b["text"], "url": b["url"]}] for b in buttons[:8]]
        payload["reply_markup"] = json.dumps({"inline_keyboard": keyboard}, ensure_ascii=False)

    r = S.post(url, data=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status()

def fetch_list_all() -> list:
    """
    Pagination loop for list.json
    """
    end_de = datetime.now().strftime("%Y%m%d")
    bgn_de = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    all_items = []
    page_no = 1
    while True:
        params = {
            "crtfc_key": DART_API_KEY,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": "B",     # 주요사항보고 중심
            "page_no": page_no,
            "page_count": 100,
            "sort": "date",
            "sort_mth": "desc",
        }
        r = S.get(LIST_URL, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()

        status = data.get("status")
        if status == "013":
            break
        if status != "000":
            raise RuntimeError(f"LIST error: {status} / {data.get('message')}")

        items = data.get("list", []) or []
        if not items:
            break

        all_items.extend(items)

        # Stop if fewer than page_count returned
        if len(items) < 100:
            break
        page_no += 1

        # safety upper bound
        if page_no > 50:
            break

    return all_items

def strip_tags_to_text(html_text: str) -> str:
    # remove scripts/styles quickly
    html_text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html_text)
    # replace <br>, <td>, <tr> with newlines to keep table-ish structure
    html_text = re.sub(r"(?i)<br\s*/?>", "\n", html_text)
    html_text = re.sub(r"(?i)</(td|th|tr|p|div|li|h\d)>", "\n", html_text)
    # remove all tags
    html_text = re.sub(r"(?is)<.*?>", " ", html_text)
    # unescape
    txt = html.unescape(html_text)
    # normalize spaces
    txt = re.sub(r"[ \t\r\f\v]+", " ", txt)
    txt = re.sub(r"\n{2,}", "\n", txt)
    return txt.strip()

def fetch_dart_view_text(rcept_no: str) -> str:
    """
    Fetch DART viewer page, extract visible text (best-effort).
    """
    url = DART_VIEW.format(rcept_no=rcept_no)
    r = S.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return strip_tags_to_text(r.text)

def extract_field(text: str, label: str) -> str:
    """
    Generic label-based extraction from text (best-effort).
    Finds: <label> ... (next token/line)
    """
    # try line-based
    # e.g. "신주배정기준일 2026년 02월 20일"
    pattern = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([^\n]+)", re.I)
    m = pattern.search(text)
    if m:
        return m.group(1).strip()

    # try "label\nvalue"
    pattern2 = re.compile(rf"{re.escape(label)}\s*\n\s*([^\n]+)", re.I)
    m2 = pattern2.search(text)
    if m2:
        return m2.group(1).strip()

    return ""

def parse_date_range(s: str) -> str:
    s = s.strip()
    if not s:
        return ""
    # clean spacing
    s = re.sub(r"\s+", " ", s)
    return s

def to_int_krw(s) -> int:
    if s is None:
        return 0
    t = str(s).strip()
    if not t:
        return 0
    t = t.replace(",", "")
    try:
        return int(float(t))
    except Exception:
        return 0

def fmt_money(v: int) -> str:
    if v <= 0:
        return ""
    return f"{v:,}원"

def fetch_pifric_detail_for_day(corp_code: str, rcept_dt: str, rcept_no: str) -> dict:
    """
    pifricDecsn: corp_code + bgn_de/end_de day range, then pick matching rcept_no.
    """
    if not corp_code or not rcept_dt:
        return {}
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": rcept_dt,
        "end_de": rcept_dt,
    }
    r = S.get(PIFRIC_URL, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    status = data.get("status")
    if status == "013":
        return {}
    if status != "000":
        return {}
    items = data.get("list", []) or []
    for it in items:
        if (it.get("rcept_no") or "").strip() == rcept_no:
            return it
    return {}

def build_financing_purpose(detail: dict) -> str:
    # OpenDART pifric fields
    fclt = to_int_krw(detail.get("piic_fdpp_fclt"))
    op   = to_int_krw(detail.get("piic_fdpp_op"))
    dtrp = to_int_krw(detail.get("piic_fdpp_dtrp"))
    ocsa = to_int_krw(detail.get("piic_fdpp_ocsa"))
    etc  = to_int_krw(detail.get("piic_fdpp_etc"))
    total = fclt + op + dtrp + ocsa + etc

    parts = []
    if fclt: parts.append(f"시설 {fmt_money(fclt)}")
    if op:   parts.append(f"운영 {fmt_money(op)}")
    if dtrp: parts.append(f"채무상환 {fmt_money(dtrp)}")
    if ocsa: parts.append(f"타법인증권취득 {fmt_money(ocsa)}")
    if etc:  parts.append(f"기타 {fmt_money(etc)}")
    if not parts:
        return ""
    if total <= 0:
        return ", ".join(parts)
    return f"{', '.join(parts)} (합계 {fmt_money(total)})"

def get_total_raise_amount(detail: dict) -> int:
    # Same fields sum
    fclt = to_int_krw(detail.get("piic_fdpp_fclt"))
    op   = to_int_krw(detail.get("piic_fdpp_op"))
    dtrp = to_int_krw(detail.get("piic_fdpp_dtrp"))
    ocsa = to_int_krw(detail.get("piic_fdpp_ocsa"))
    etc  = to_int_krw(detail.get("piic_fdpp_etc"))
    return fclt + op + dtrp + ocsa + etc

def fetch_stock_code(corp_code: str) -> str:
    if not corp_code:
        return ""
    params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code}
    r = S.get(COMPANY_URL, params=params, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        return ""
    try:
        data = r.json()
    except Exception:
        return ""
    if data.get("status") != "000":
        return ""
    sc = (data.get("stock_code") or "").strip()
    return sc

def fetch_naver_price_mcap(stock_code: str):
    """
    Best-effort scrape from Naver Finance item page.
    Returns (cur_price:int, market_cap:int)
    """
    if not stock_code:
        return (0, 0)
    url = NAVER_ITEM.format(stock_code=stock_code)
    r = S.get(url, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        return (0, 0)
    txt = r.text

    # Current price: "no_today" area contains <span class="blind">숫자</span>
    m_price = re.search(r'no_today[^<]*</span>\s*<span[^>]*class="blind"[^>]*>\s*([\d,]+)\s*</span>', txt, re.I)
    cur = int(m_price.group(1).replace(",", "")) if m_price else 0

    # Market cap: look for label "시가총액" then next <em>...<span class="blind">숫자</span>
    # Naver often shows unit "억원"
    # We'll parse number and unit, convert to KRW.
    m_mcap_block = re.search(r'시가총액\s*</th>\s*<td[^>]*>(.*?)</td>', txt, re.I | re.S)
    mcap = 0
    if m_mcap_block:
        block = m_mcap_block.group(1)
        m_num = re.search(r'class="blind"\s*>\s*([\d,]+)\s*</span>', block, re.I)
        if m_num:
            # Naver blind typically already in "억원" number (e.g., 12조 3456억 is tricky)
            # If it’s plain number with "억원" around, assume 억원.
            num = int(m_num.group(1).replace(",", ""))
            # Try detect "억원"
            if "억원" in block:
                mcap = num * 100_000_000
            else:
                # fallback assume KRW
                mcap = num
    return (cur, mcap)

def clamp01(x: float) -> float:
    if x < 0: return 0.0
    if x > 1: return 1.0
    return x

def compute_risk_score(raise_amt: int, offer_price: int, cur_price: int, mcap: int, body_text: str):
    """
    투자 위험도(0~100) 느낌의 점수.
    - size_ratio = raise/mcap (0~0.3 이상이면 큰 편)
    - discount   = (cur-offer)/cur (0~0.3 이상이면 큰 편)
    - 최대주주 참여 힌트가 있으면 -10 (리스크 완화)
    """
    if mcap <= 0 and cur_price <= 0:
        return (None, "")

    size_ratio = (raise_amt / mcap) if (raise_amt > 0 and mcap > 0) else None
    discount = ((cur_price - offer_price) / cur_price) if (offer_price > 0 and cur_price > 0) else None

    score = 30.0
    parts = []

    if size_ratio is not None:
        # 0.30(30%) 이상이면 강하게 가산
        s = clamp01(size_ratio / 0.30)
        score += 40.0 * s
        parts.append(f"시총대비 {size_ratio*100:.1f}%")

    if discount is not None:
        d = clamp01(discount / 0.30)
        score += 30.0 * d
        parts.append(f"할인율 {discount*100:.1f}%")

    # 최대주주 참여 힌트 (정교하진 않지만 “있음” 텍스트 기반)
    if re.search(r"최대주주.*(참여|청약|인수)", body_text, re.I):
        score -= 10.0
        parts.append("최대주주참여?=추정")

    score = max(0, min(100, int(round(score))))
    level = "LOW" if score < 40 else ("MID" if score < 70 else "HIGH")
    detail = f"{level} ({score})" + (f" / {', '.join(parts)}" if parts else "")
    return (score, detail)

def html_escape(s: str) -> str:
    return html.escape(s or "", quote=False)

def add_line(lines, label, value):
    v = (value or "").strip()
    if not v or v.upper() == "N/A":
        return
    lines.append(f"• <b>{html_escape(label)}</b>: {html_escape(v)}")

def should_include(item: dict, body_text: str) -> bool:
    rpt_nm = (item.get("report_nm") or "").strip()
    if not INC_RE.search(rpt_nm):
        return False

    # Exclude 3rd-party by title or body
    if EXC_RE_TITLE.search(rpt_nm):
        return False
    if body_text and EXC_RE_BODY.search(body_text):
        return False

    # Market class filter (optional)
    # DART list provides "corp_cls": Y/K/N/E (Y=유가, K=코스닥, N=코넥스, E=기타)
    corp_cls = (item.get("corp_cls") or "").strip().upper()
    # accept legacy mapping: sometimes users set "K" meaning KOSDAQ and "Y" meaning KOSPI
    if MARKET_CLASSES:
        if corp_cls not in MARKET_CLASSES:
            # Also accept if user provided "KOSDAQ"/"KOSPI"
            # but keep simple: corp_cls must match list.
            return False

    # Prefer shareholder/general hints when available, but don't hard-block
    # (some filings omit these strings)
    return True

def group_by_company(items: list):
    """
    Group items by corp_code, keeping original order.
    """
    groups = []
    by = {}
    for it in items:
        corp_code = (it.get("corp_code") or "").strip()
        if not corp_code:
            continue
        if corp_code not in by:
            by[corp_code] = {"corp_code": corp_code, "corp_name": it.get("corp_name") or "N/A", "corp_cls": it.get("corp_cls") or "", "items": []}
            groups.append(by[corp_code])
        by[corp_code]["items"].append(it)
    return groups

def main():
    must_env()
    st = load_state()

    # 1) fetch list
    items = fetch_list_all()
    # They come in desc; we send oldest-first for nicer flow
    items = list(reversed(items))

    # 2) prefetch body text only when needed (filtering / extracting)
    selected = []
    for it in items:
        rcept_no = (it.get("rcept_no") or "").strip()
        if not rcept_no:
            continue
        if seen_has(st, rcept_no):
            continue

        body_text = ""
        try:
            body_text = fetch_dart_view_text(rcept_no)
        except Exception:
            body_text = ""

        if not should_include(it, body_text):
            continue

        # Extra filter: exclude if ONLY 3rd-party vibes in body and no shareholder/general hints
        # (still already excluded by EXC_RE_BODY, so this is just extra safety)
        selected.append((it, body_text))

    if not selected:
        print("OK sent=0 seen=%d" % len(st.get("seen", [])))
        save_state(st)
        return

    # 3) group by company (same run batch)
    # Also keep only unseen list already
    grouped_input = []
    for it, body in selected:
        it2 = dict(it)
        it2["_body_text"] = body
        grouped_input.append(it2)

    groups = group_by_company(grouped_input)

    sent_count = 0

    for g in groups:
        corp_name = g["corp_name"]
        corp_cls = (g.get("corp_cls") or "").strip().upper()
        market = "KOSPI" if corp_cls == "Y" else ("KOSDAQ" if corp_cls == "K" else ("KONEX" if corp_cls == "N" else corp_cls or "N/A"))

        # Build card header
        lines = []
        lines.append("📌 <b>증자 공시 감지</b>")
        lines.append(f"• <b>회사</b>: {html_escape(corp_name)} ({html_escape(market)})")

        # sort items by date/time if possible; keep current order
        reports = []
        buttons = []

        # If multiple, we'll include up to 3 buttons and keep links in text too
        for it in g["items"]:
            rcept_no = (it.get("rcept_no") or "").strip()
            rpt_nm = (it.get("report_nm") or "").strip()
            rcept_dt = (it.get("rcept_dt") or "").strip()

            url = DART_VIEW.format(rcept_no=rcept_no)
            reports.append((rcept_dt, rpt_nm, rcept_no, url))

        # Build per-report details (first report only for heavy detail)
        # (If you want per-report detail later, expand here)
        details_block = []
        if reports:
            # Use the first report for detail extraction
            rcept_dt, rpt_nm, rcept_no, url = reports[0]
            body_text = ""
            for it in g["items"]:
                if (it.get("rcept_no") or "").strip() == rcept_no:
                    body_text = it.get("_body_text") or ""
                    break

            # OpenDART detail (유상증자결정 only; for 무상/유무상 might be empty -> OK)
            corp_code = g["corp_code"]
            detail = {}
            try:
                detail = fetch_pifric_detail_for_day(corp_code, rcept_dt, rcept_no)
            except Exception:
                detail = {}

            # Financing purpose + raise amount
            purpose = build_financing_purpose(detail) if detail else ""
            raise_amt = get_total_raise_amount(detail) if detail else 0

            # Extract schedule-ish fields from viewer text
            record_date = extract_field(body_text, "신주배정기준일")
            offer_price = extract_field(body_text, "예정발행가액") or extract_field(body_text, "발행가액(예정)") or extract_field(body_text, "1주당 발행가액")
            confirm_date = extract_field(body_text, "발행가액 확정일") or extract_field(body_text, "확정발행가액 결정일") or extract_field(body_text, "확정일")
            rights_list_period = extract_field(body_text, "신주인수권증서 상장예정기간") or extract_field(body_text, "신주인수권상장예정기간")
            subs_period = extract_field(body_text, "청약일") or extract_field(body_text, "구주주 청약일") or extract_field(body_text, "일반공모 청약일")
            listing_date = extract_field(body_text, "신주의 상장예정일") or extract_field(body_text, "신주 상장예정일")

            # Clean ranges
            record_date = parse_date_range(record_date)
            confirm_date = parse_date_range(confirm_date)
            rights_list_period = parse_date_range(rights_list_period)
            subs_period = parse_date_range(subs_period)
            listing_date = parse_date_range(listing_date)

            # Convert offer price to int (best-effort)
            offer_price_int = 0
            if offer_price:
                m = re.search(r"([\d,]+)\s*원", offer_price)
                if m:
                    offer_price_int = int(m.group(1).replace(",", ""))
                else:
                    offer_price_int = to_int_krw(offer_price)

            # Amount filter (MIN_RAISE_KRW)
            if MIN_RAISE_KRW > 0 and raise_amt > 0 and raise_amt < MIN_RAISE_KRW:
                # mark all items as seen so it won't spam later
                for _, _, rno, _ in reports:
                    seen_add(st, rno)
                continue

            # Risk score (optional; needs stock_code -> naver)
            risk_line = ""
            cur_price = 0
            mcap = 0
            try:
                stock_code = fetch_stock_code(corp_code)
                if stock_code:
                    cur_price, mcap = fetch_naver_price_mcap(stock_code)
                score, risk_detail = compute_risk_score(raise_amt, offer_price_int, cur_price, mcap, body_text)
                if risk_detail:
                    risk_line = risk_detail
            except Exception:
                risk_line = ""

            # Add details block (hide N/A)
            if purpose:
                add_line(details_block, "자금조달 목적", purpose)
            if raise_amt > 0:
                add_line(details_block, "조달금액(추정)", fmt_money(raise_amt).replace("원"," 원"))
            if record_date:
                add_line(details_block, "신주배정기준일", record_date)
            if offer_price and offer_price_int > 0:
                add_line(details_block, "예정가(1주)", f"{offer_price_int:,}원")
            elif offer_price:
                add_line(details_block, "예정가", offer_price)
            if confirm_date:
                add_line(details_block, "확정일", confirm_date)
            if rights_list_period:
                add_line(details_block, "신주인수권 상장예정기간", rights_list_period)
            if subs_period:
                add_line(details_block, "청약일", subs_period)
            if listing_date:
                add_line(details_block, "신주 상장예정일", listing_date)
            if risk_line:
                add_line(details_block, "위험도(추정)", risk_line)

        # Reports list
        lines.append("")
        lines.append("<b>공시</b>")
        for (rcept_dt, rpt_nm, rcept_no, url) in reports:
            # Show date + title (and rcept_no)
            lines.append(f"– {html_escape(rcept_dt)}  <a href=\"{html_escape(url)}\">{html_escape(rpt_nm)}</a> ({html_escape(rcept_no)})")

        if details_block:
            lines.append("")
            lines.append("<b>요약</b>")
            lines.extend(details_block)

        # Buttons: show up to 3 DART links
        for i, (_, rpt_nm, rcept_no, url) in enumerate(reports[:3]):
            btn_text = "📄 DART 열기" if i == 0 else f"📄 DART 열기 {i+1}"
            buttons.append({"text": btn_text, "url": url})

        msg = "\n".join(lines)

        # Send
        tg_send_html(msg, buttons=buttons)

        # Mark seen
        for _, _, rno, _ in reports:
            seen_add(st, rno)
        sent_count += 1

        # avoid rate-limit
        time.sleep(0.7)

    save_state(st)
    print(f"OK sent={sent_count} seen={len(st.get('seen', []))}")

if __name__ == "__main__":
    main()
