#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, html, time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import requests

# =========================
# ENV
# =========================
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
PAGE_COUNT    = int(os.getenv("PAGE_COUNT", "100"))
MAX_PAGES     = int(os.getenv("MAX_PAGES", "30"))  # pagination cap
STATE_PATH    = os.getenv("STATE_PATH", "state.json")

# 시장 필터 (K=KOSPI, Q=KOSDAQ, N=KONEX) - list.json 자체에 market 구분이 직접 안 나올 수 있어 body에서 추출
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "K,Q,N").split(",") if x.strip()]

# 증자/무상/유무상 관련 공시명 매칭(제목)
INC_TITLE_RE = re.compile(r"(유상증자|무상증자|유무상증자)", re.I)

# "제3자배정"은 무조건 제외 (제목/본문)
EXC_3RD_RE = re.compile(r"제\s*3\s*자\s*배정", re.I)

# 포함 조건(본문): 일반공모/일반주주/주주배정/구주주/기존주주 등
INC_BODY_RE = re.compile(r"(주주배정|구주주|기존주주|일반공모|일반주주|공모)", re.I)

# DART API
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
VIEW_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"
TG_SEND  = "https://api.telegram.org/bot{token}/sendMessage"

# Telegram limits: keep well under 4096
TG_MAX = 3500

UA = {"User-Agent": "dart-alert-actions/2.0"}

# =========================
# Utils
# =========================
def must_env():
    missing = []
    if not DART_API_KEY: missing.append("DART_API_KEY")
    if not TG_BOT_TOKEN: missing.append("TG_BOT_TOKEN")
    if not TG_CHAT_ID:   missing.append("TG_CHAT_ID")
    if missing:
        raise SystemExit(f"[ERROR] Missing env: {', '.join(missing)}")

def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        if not isinstance(st, dict): raise ValueError("state not dict")
        st.setdefault("seen", [])
        st.setdefault("seen_set", {})  # optional cache
        return st
    except Exception:
        return {"seen": [], "seen_set": {}}

def save_state(st: Dict[str, Any]) -> None:
    # keep only last N seen to avoid repo bloat
    seen = st.get("seen", [])
    if len(seen) > 4000:
        st["seen"] = seen[-4000:]
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"seen": st.get("seen", [])}, f, ensure_ascii=False, indent=2)

def is_seen(st: Dict[str, Any], rcept_no: str) -> bool:
    # use list for persistence; build set on the fly for speed
    seen_list = st.get("seen", [])
    if "seen_set" not in st or not st["seen_set"]:
        st["seen_set"] = {x: True for x in seen_list}
    return bool(st["seen_set"].get(rcept_no))

def mark_seen(st: Dict[str, Any], rcept_no: str) -> None:
    if is_seen(st, rcept_no): 
        return
    st["seen"].append(rcept_no)
    st["seen_set"][rcept_no] = True

def tg_send_html(text: str, button_url: Optional[str] = None) -> None:
    url = TG_SEND.format(token=TG_BOT_TOKEN)
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if button_url:
        # inline keyboard
        payload["reply_markup"] = json.dumps({
            "inline_keyboard": [[{"text": "📄 DART 열기", "url": button_url}]]
        }, ensure_ascii=False)
    r = requests.post(url, data=payload, timeout=25)
    r.raise_for_status()

def clamp_text(s: str, n: int = TG_MAX) -> str:
    if len(s) <= n:
        return s
    return s[: n-30] + "\n…(길이 제한으로 일부 생략)…"

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_value(v: str) -> str:
    if not v:
        return ""
    t = norm_space(v)
    if t in ("-", "N/A", "n/a", "NA", "na"):
        return ""
    return t

def add_line(lines: List[str], k: str, v: str) -> None:
    v2 = clean_value(v)
    if not v2:
        return
    lines.append(f"• <b>{html.escape(k)}</b>: {html.escape(v2)}")

def money_to_int(s: str) -> int:
    if not s:
        return 0
    t = re.sub(r"[^\d]", "", s)
    try:
        return int(t) if t else 0
    except Exception:
        return 0

def fmt_krw(n: int) -> str:
    if n <= 0:
        return ""
    return f"{n:,}원"

# =========================
# DART fetch (pagination)
# =========================
def fetch_disclosures() -> List[Dict[str, Any]]:
    end_de = datetime.now().strftime("%Y%m%d")
    bgn_de = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    all_items: List[Dict[str, Any]] = []

    page_no = 1
    while page_no <= MAX_PAGES:
        params = {
            "crtfc_key": DART_API_KEY,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": "B",        # 주요사항보고서 위주
            "page_no": page_no,
            "page_count": PAGE_COUNT,
            "sort": "date",
            "sort_mth": "desc",
        }
        r = requests.get(LIST_URL, params=params, headers=UA, timeout=25)
        r.raise_for_status()
        data = r.json()

        status = data.get("status")
        if status == "013":
            break
        if status != "000":
            raise RuntimeError(f"DART list error: {status} / {data.get('message')}")

        items = data.get("list", []) or []
        if not items:
            break

        all_items.extend(items)

        # if less than page_count, it's last page
        if len(items) < PAGE_COUNT:
            break
        page_no += 1

    return all_items

# =========================
# Report HTML fetch + field extraction
# =========================
def fetch_report_html(rcept_no: str) -> str:
    """Best-effort: fetch main viewer HTML text. It contains some searchable text and labels."""
    url = VIEW_URL.format(rcept_no=rcept_no)
    r = requests.get(url, headers=UA, timeout=25)
    r.raise_for_status()
    return r.text

def extract_field(body_html: str, label: str) -> str:
    """
    매우 러프한 라벨 추출:
    - 모바일/웹에서 표/라벨이 HTML로 섞여 들어오므로 'label ... 값' 형태를 정규식으로 잡음
    - 실패하면 빈값 -> 자동 숨김
    """
    if not body_html:
        return ""
    text = html.unescape(body_html)
    # 태그 제거(간단)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)

    # label 뒤에 값이 따라오는 패턴(최대 80자)
    # 예: "자금조달의 목적 기타자금(원) 3,138,000,000"
    pat = re.compile(rf"{re.escape(label)}\s*[:：]?\s*([^\n\r]{{1,80}})", re.I)
    m = pat.search(text)
    if not m:
        return ""
    val = m.group(1)
    # 다음 라벨/표 헤더로 이어지는 흔한 잡음 컷
    val = re.split(r"(보통주식|기타주식|원\)|주\)|예정|확정|기준일|상장|청약|기간)", val)[0]
    return val.strip()

def extract_market_hint(body_html: str) -> str:
    """
    KOSPI/KOSDAQ/KONEX 힌트 추출(없으면 빈값).
    """
    if not body_html:
        return ""
    t = body_html
    if re.search(r"KOSDAQ", t, re.I): return "KOSDAQ"
    if re.search(r"KOSPI", t, re.I):  return "KOSPI"
    if re.search(r"KONEX", t, re.I):  return "KONEX"
    return ""

def should_include(rpt_nm: str, body_html: str) -> bool:
    # 1) 제목에 증자/무상/유무상 포함 필수
    if not rpt_nm or not INC_TITLE_RE.search(rpt_nm):
        return False

    # 2) 제3자배정은 무조건 제외(제목/본문)
    if EXC_3RD_RE.search(rpt_nm or ""):
        return False
    if body_html and EXC_3RD_RE.search(body_html):
        return False

    # 3) 본문에 "주주/일반" 힌트가 있어야 포함
    # (이 조건 때문에 "유상증자결정"인데 배정방식 표기가 없는 케이스가 빠질 수 있음.
    #  그런 케이스를 포함하고 싶으면 이 조건을 완화해줄 수 있음.)
    if not (body_html and INC_BODY_RE.search(body_html)):
        return False

    return True

# =========================
# Risk score (simple, disclosure-based)
# =========================
def compute_risk_score(raise_amt_krw: int, discount_pct: Optional[float] = None, mc_ratio: Optional[float] = None) -> Tuple[int, str]:
    """
    0~100 스코어(간단형)
    - 조달금액(원) 크면 점수↑
    - 할인율(%) 크면 점수↑ (옵션)
    - 시총대비(%) 크면 점수↑ (옵션)
    """
    score = 0

    # raise amount bucket
    if raise_amt_krw >= 300_000_000_000: score += 50
    elif raise_amt_krw >= 100_000_000_000: score += 40
    elif raise_amt_krw >= 30_000_000_000:  score += 30
    elif raise_amt_krw >= 10_000_000_000:  score += 20
    elif raise_amt_krw >= 3_000_000_000:   score += 12
    elif raise_amt_krw >= 1_000_000_000:   score += 8
    elif raise_amt_krw > 0:                score += 5

    if discount_pct is not None:
        if discount_pct >= 30: score += 25
        elif discount_pct >= 20: score += 18
        elif discount_pct >= 10: score += 10
        elif discount_pct > 0: score += 5

    if mc_ratio is not None:
        # mc_ratio in percent
        if mc_ratio >= 50: score += 25
        elif mc_ratio >= 30: score += 18
        elif mc_ratio >= 15: score += 12
        elif mc_ratio >= 5: score += 6

    score = max(0, min(100, score))

    if score >= 70: label = "🔴 높음"
    elif score >= 40: label = "🟠 보통"
    else: label = "🟢 낮음"
    return score, label

# =========================
# Grouping + Message
# =========================
def build_card(company: str, market: str, rcept_dt: str, rpt_nm: str, rcept_no: str, body_html: str) -> Tuple[str, str]:
    url = VIEW_URL.format(rcept_no=rcept_no)

    lines: List[str] = []
    lines.append(f"📌 <b>증자 공시 감지</b>")
    lines.append(f"• <b>회사</b>: {html.escape(company)}" + (f" ({html.escape(market)})" if market else ""))
    lines.append(f"• <b>접수일</b>: {html.escape(rcept_dt)}")
    lines.append("")  # blank

    # headline
    lines.append(f"<b>공시</b>")
    lines.append(f"– {html.escape(rpt_nm)}")
    lines.append(f"(<code>{html.escape(rcept_no)}</code>)")

    # Details (N/A auto-hide)
    d: List[str] = []
    # 요청 필드들
    add_line(d, "자금조달의 목적", extract_field(body_html, "자금조달의 목적"))
    add_line(d, "신주배정기준일", extract_field(body_html, "신주배정기준일"))
    add_line(d, "예정가", extract_field(body_html, "예정발행가액") or extract_field(body_html, "예정가액") or extract_field(body_html, "예정가"))
    add_line(d, "확정일", extract_field(body_html, "확정발행가액") or extract_field(body_html, "확정일"))
    add_line(d, "신주인수권상장예정기간", extract_field(body_html, "신주인수권증서 상장예정기간") or extract_field(body_html, "신주인수권상장예정기간"))
    add_line(d, "청약일", extract_field(body_html, "청약일") or extract_field(body_html, "청약기간"))
    add_line(d, "신주의상장예정일", extract_field(body_html, "신주의 상장예정일") or extract_field(body_html, "신주 상장예정일") or extract_field(body_html, "상장예정일"))

    # 조달금액(원) 추정 (본문에서 "기타자금(원)" 같은 항목이 잡히면 숫자 인식)
    # 여러 칸이 있을 수 있어 가장 큰 숫자 하나를 조달금액으로 사용(간단형)
    raise_candidates = re.findall(r"(\d{1,3}(?:,\d{3})+)\s*원", html.unescape(re.sub(r"<[^>]+>", " ", body_html or "")))
    raise_amt = 0
    for c in raise_candidates:
        raise_amt = max(raise_amt, money_to_int(c))
    if raise_amt > 0:
        add_line(d, "조달금액(추정)", fmt_krw(raise_amt))

    # Risk (simple)
    score, label = compute_risk_score(raise_amt if raise_amt else 0)
    d.append(f"• <b>위험도</b>: {label} (<b>{score}</b>/100)")

    if d:
        lines.append("")
        lines.append("<b>요약</b>")
        lines.extend(d)

    text = clamp_text("\n".join(lines))
    return text, url

def group_items(items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    같은 회사(corp_name) 기준 묶기.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        corp = (it.get("corp_name") or "N/A").strip()
        grouped.setdefault(corp, []).append(it)
    return grouped

# =========================
# Main
# =========================
def main():
    must_env()
    st = load_state()

    items = fetch_disclosures()

    # 최신순으로 오지만, 발송은 오래된 것부터(역순)
    items = list(reversed(items))

    send_count = 0
    candidate: List[Dict[str, Any]] = []

    # 1) 수집 + 필터링
    for it in items:
        rpt_nm = (it.get("report_nm") or "").strip()
        if not rpt_nm:
            continue
        if not INC_TITLE_RE.search(rpt_nm):
            continue

        rcept_no = (it.get("rcept_no") or "").strip()
        if not rcept_no:
            continue
        if is_seen(st, rcept_no):
            continue

        # HTML fetch (best-effort)
        body_html = ""
        try:
            body_html = fetch_report_html(rcept_no)
        except Exception:
            body_html = ""

        # include/exclude 판단
        if not should_include(rpt_nm, body_html):
            # 제외 처리도 seen으로 찍어두면 같은 공시로 계속 재시도 안 함
            mark_seen(st, rcept_no)
            continue

        # 시장 힌트
        market = extract_market_hint(body_html)
        if market:
            # MARKET_CLASSES 필터
            if market == "KOSPI" and "K" not in MARKET_CLASSES: 
                mark_seen(st, rcept_no); continue
            if market == "KOSDAQ" and "Q" not in MARKET_CLASSES:
                mark_seen(st, rcept_no); continue
            if market == "KONEX" and "N" not in MARKET_CLASSES:
                mark_seen(st, rcept_no); continue

        it["_body_html"] = body_html
        it["_market"] = market
        candidate.append(it)

    if not candidate:
        print(f"OK sent=0 seen={len(st.get('seen', []))}")
        save_state(st)
        return

    # 2) 회사별 묶기
    grouped = group_items(candidate)

    # 3) 발송(회사당 1건 메시지로 묶어서)
    for corp, its in grouped.items():
        # 최신 10개까지만 표시
        its = its[-10:]

        # 카드 1장 + 하단에 공시 리스트(여러 건)
        first = its[0]
        rcept_dt = (first.get("rcept_dt") or "").strip()
        rpt_nm   = (first.get("report_nm") or "").strip()
        rcept_no = (first.get("rcept_no") or "").strip()
        market   = first.get("_market", "")

        card_text, card_url = build_card(
            company=corp,
            market=market,
            rcept_dt=rcept_dt,
            rpt_nm=rpt_nm,
            rcept_no=rcept_no,
            body_html=first.get("_body_html", "")
        )

        # 여러 공시가 있으면 리스트 추가
        if len(its) > 1:
            extra_lines = ["", "<b>같은 회사 추가 공시</b>"]
            for x in its[1:]:
                extra_lines.append(f"• {html.escape((x.get('rcept_dt') or '').strip())} – {html.escape((x.get('report_nm') or '').strip())} (<code>{html.escape((x.get('rcept_no') or '').strip())}</code>)")
            card_text = clamp_text(card_text + "\n" + "\n".join(extra_lines))

        tg_send_html(card_text, button_url=card_url)
        send_count += 1

        # seen 처리
        for x in its:
            mark_seen(st, (x.get("rcept_no") or "").strip())

    save_state(st)
    print(f"OK sent={send_count} seen={len(st.get('seen', []))}")

if __name__ == "__main__":
    main()
