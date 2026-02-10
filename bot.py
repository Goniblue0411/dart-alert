#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, html, time
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

# ========= ENV =========
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))
# 시장구분 필터: K=KOSDAQ, Y=KOSPI, N=KONEX (네가 쓰던 방식 유지)
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "Y,K").split(",") if x.strip()]

# 최소 조달금액 필터(원) - 0이면 필터 없음
MIN_RAISE_KRW = int(os.getenv("MIN_RAISE_KRW", "0"))

# 상태 파일 (GitHub Actions에서 repo에 커밋되는 파일)
STATE_PATH = os.getenv("STATE_PATH", "state.json")

# ========= DART endpoints =========
LIST_URL   = "https://opendart.fss.or.kr/api/list.json"
PIFRIC_URL = "https://opendart.fss.or.kr/api/pifricDecsn.json"  # 유상증자(납입자본 증가) 상세
VIEW_URL   = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

# ========= Telegram =========
TG_SEND = "https://api.telegram.org/bot{token}/sendMessage"

# ========= 세션 =========
S = requests.Session()
S.headers.update({"User-Agent": "dart-alert-actions/2.0"})

def must_env():
    missing = []
    if not DART_API_KEY: missing.append("DART_API_KEY")
    if not TG_BOT_TOKEN: missing.append("TG_BOT_TOKEN")
    if not TG_CHAT_ID: missing.append("TG_CHAT_ID")
    if missing:
        raise SystemExit(f"[ERROR] Missing env: {', '.join(missing)}")

# ---------------- STATE ----------------
def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        if "seen" not in st or not isinstance(st["seen"], list):
            st["seen"] = []
        return st
    except Exception:
        return {"seen": []}

def save_state(st: Dict[str, Any]):
    # seen이 너무 커지는 것 방지(최신 5000개 유지)
    st["seen"] = st.get("seen", [])[-5000:]
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def is_seen(st: Dict[str, Any], rcept_no: str) -> bool:
    return rcept_no in set(st.get("seen", []))

def mark_seen(st: Dict[str, Any], rcept_no: str):
    st.setdefault("seen", []).append(rcept_no)

# ---------------- HELPERS ----------------
def _to_int(x) -> int:
    if x is None: return 0
    s = str(x).strip()
    if not s: return 0
    try:
        return int(s.replace(",", ""))
    except Exception:
        return 0

def _fmt_money(v: int) -> str:
    return f"{v:,}원" if v > 0 else ""

def _fmt_date(yyyymmdd: str) -> str:
    s = (yyyymmdd or "").strip()
    if re.fullmatch(r"\d{8}", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def _clean(s: str) -> str:
    return (s or "").strip()

def _nonempty_lines(items: List[Tuple[str, str]]) -> List[str]:
    # ("라벨", "값") -> 값이 비면 제외
    out = []
    for k, v in items:
        v = _clean(v)
        if v:
            out.append(f"• <b>{html.escape(k)}</b>: {html.escape(v)}")
    return out

# ---------------- FILTERS ----------------
# 포함: 일반공모/주주배정 + 유상증자/무상증자
INC_RPT = re.compile(r"(유상증자결정|무상증자결정|유무상증자결정)", re.I)

# 제외: 제3자배정(보고서명에 나오거나, 상세의 증자방식에 나오면 컷)
EXC_THIRD = re.compile(r"(제\s*3\s*자|제3자)\s*배정", re.I)

def match_report_name(rpt_nm: str) -> bool:
    rpt_nm = rpt_nm or ""
    if not INC_RPT.search(rpt_nm):
        return False
    # 제목에 제3자배정 명시되면 바로 제외
    if EXC_THIRD.search(rpt_nm):
        return False
    return True

def market_ok(market: str) -> bool:
    m = (market or "").strip().upper()
    if not MARKET_CLASSES:
        return True
    return m in set(MARKET_CLASSES)

# ---------------- DART API ----------------
def fetch_list_page(page_no: int, bgn_de: str, end_de: str) -> Dict[str, Any]:
    params = {
        "crtfc_key": DART_API_KEY,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "pblntf_ty": "B",      # 주요사항보고 중심
        "page_no": page_no,
        "page_count": 100,
        "sort": "date",
        "sort_mth": "desc",
    }
    r = S.get(LIST_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_disclosures_paginated() -> List[Dict[str, Any]]:
    end_de = datetime.now().strftime("%Y%m%d")
    bgn_de = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    all_items: List[Dict[str, Any]] = []
    page_no = 1

    while True:
        data = fetch_list_page(page_no, bgn_de, end_de)
        status = data.get("status")

        if status == "013":  # 데이터 없음
            break
        if status != "000":
            raise RuntimeError(f"DART list error: {status} / {data.get('message')}")

        items = data.get("list", []) or []
        all_items.extend(items)

        # total_page 있으면 사용, 없으면 items 길이로 종료 판단
        total_page = _to_int(data.get("total_page"))
        if total_page > 0:
            if page_no >= total_page:
                break
        else:
            if len(items) < 100:
                break

        page_no += 1
        if page_no > 50:  # 안전장치
            break

    return all_items

def fetch_pifric_detail(corp_code: str, rcept_dt: str, rcept_no: str) -> Optional[Dict[str, Any]]:
    # pifricDecsn: corp_code + 날짜 범위로 조회 후 rcept_no 매칭
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": rcept_dt,
        "end_de": rcept_dt,
    }
    r = S.get(PIFRIC_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()

    status = data.get("status")
    if status == "013":
        return None
    if status != "000":
        return None

    for it in (data.get("list", []) or []):
        if (it.get("rcept_no") or "").strip() == rcept_no:
            return it
    return None

# ---------------- DETAIL EXTRACTION ----------------
def detect_third_party_from_detail(detail: Dict[str, Any]) -> bool:
    # 증자방식 필드 후보들(케이스별 다를 수 있어 여러 후보를 체크)
    candidates = [
        detail.get("piic_ic_mthn"),  # 증자방식(기존 코드)
        detail.get("asgm_mth"),      # (가정) 배정방식
        detail.get("alloc_mth"),
        detail.get("rdemptn_mth"),
    ]
    text = " ".join([str(x) for x in candidates if x])
    return bool(EXC_THIRD.search(text))

def extract_raise_amount(detail: Dict[str, Any]) -> int:
    # 자금사용목적 합계(기존 코드 방식)
    keys = ["piic_fdpp_fclt", "piic_fdpp_op", "piic_fdpp_dtrp", "piic_fdpp_ocsa", "piic_fdpp_etc"]
    total = 0
    for k in keys:
        total += _to_int(detail.get(k))
    return total

def extract_purposes_text(detail: Dict[str, Any]) -> str:
    mapping = [
        ("시설", "piic_fdpp_fclt"),
        ("운영", "piic_fdpp_op"),
        ("채무상환", "piic_fdpp_dtrp"),
        ("타법인증권취득", "piic_fdpp_ocsa"),
        ("기타", "piic_fdpp_etc"),
    ]
    parts = []
    for name, k in mapping:
        v = _to_int(detail.get(k))
        if v:
            parts.append(f"{name} {_fmt_money(v)}")
    return ", ".join(parts)

def safe_pick(detail: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = detail.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("null", "none", "n/a"):
            return s
    return ""

def build_card_html(group_key: Tuple[str, str, str], items: List[Dict[str, Any]]) -> Tuple[str, str]:
    """
    group_key: (corp_name, market, rcept_dt)
    items: same corp/date items
    returns: (text_html, button_url)
    """
    corp_name, market, rcept_dt = group_key

    # 대표 링크: 가장 최신 rcept_no 하나
    newest = sorted(items, key=lambda x: (x.get("rcept_no") or ""), reverse=True)[0]
    button_url = VIEW_URL.format(rcept_no=(newest.get("rcept_no") or "").strip())

    # 공시 제목 리스트
    titles = []
    for it in items:
        rpt = _clean(it.get("report_nm", ""))
        rno = _clean(it.get("rcept_no", ""))
        if rpt:
            titles.append(f"– {html.escape(rpt)} <code>({html.escape(rno)})</code>")
    titles = list(dict.fromkeys(titles))  # 중복 제거

    # 상세(유상증자만: pifric 가능할 때)
    detail = newest.get("_detail") or {}

    # 핵심 필드(가능할 때만 표시)
    inc_method = safe_pick(detail, ["piic_ic_mthn"])
    purposes   = extract_purposes_text(detail)
    raise_amt  = extract_raise_amount(detail)

    # 날짜/일정 관련: API 키 이름이 케이스별로 달라질 수 있어 후보군으로 “시도”
    base_dt = _fmt_date(safe_pick(detail, ["piic_stk_asgn_std_dt", "asgn_std_dt", "stk_asgn_std_dt"]))
    sub_bgn = _fmt_date(safe_pick(detail, ["piic_sbmsn_bgn_dt", "subscrptn_bgn_de", "sub_bgn_dt"]))
    sub_end = _fmt_date(safe_pick(detail, ["piic_sbmsn_end_dt", "subscrptn_end_de", "sub_end_dt"]))
    price_pln = safe_pick(detail, ["piic_expc_prc", "expc_prc", "plan_prc", "piic_prc"])
    price_fix_dt = _fmt_date(safe_pick(detail, ["piic_prc_dcsn_dt", "prc_dcsn_dt", "fix_prc_dt"]))
    listing_dt = _fmt_date(safe_pick(detail, ["piic_lstn_pln_dt", "lstn_pln_dt", "new_stk_lstn_dt"]))

    # “신주인수권 상장 기간”도 후보로 시도 (정확키는 공시/응답에 따라 달라질 수 있음)
    right_bgn = _fmt_date(safe_pick(detail, ["piic_newstk_rgt_lstn_bgn_dt", "rgt_lstn_bgn_dt"]))
    right_end = _fmt_date(safe_pick(detail, ["piic_newstk_rgt_lstn_end_dt", "rgt_lstn_end_dt"]))
    right_period = ""
    if right_bgn or right_end:
        right_period = f"{right_bgn} ~ {right_end}".strip(" ~")

    # 조달금액 필터
    if MIN_RAISE_KRW > 0 and raise_amt and raise_amt < MIN_RAISE_KRW:
        # 필터에 걸리면 “표시만 안 하고” 상위에서 제외 처리하도록 빈값 반환
        pass

    # 카드 구성 (N/A 자동 숨김)
    header = f"📌 <b>증자 공시 감지</b>\n• <b>회사</b>: {html.escape(corp_name)} <b>({html.escape(market)})</b>\n• <b>접수일</b>: {html.escape(_fmt_date(rcept_dt))}"

    # 일정 묶어서 “청약일정” 표시
    subscrptn = ""
    if sub_bgn or sub_end:
        subscrptn = f"{sub_bgn} ~ {sub_end}".strip(" ~")

    info_lines = _nonempty_lines([
        ("증자방식", inc_method),
        ("자금조달 목적", purposes),
        ("조달금액(목적합계)", _fmt_money(raise_amt) if raise_amt else ""),
        ("신주배정기준일", base_dt),
        ("예정가", price_pln),
        ("확정일", price_fix_dt),
        ("신주인수권 상장예정기간", right_period),
        ("청약일", subscrptn),
        ("신주의 상장예정일", listing_dt),
    ])

    body = ""
    if titles:
        body += "\n\n<b>공시</b>\n" + "\n".join(titles)

    if info_lines:
        body += "\n\n<b>요약</b>\n" + "\n".join(info_lines)

    text_html = header + body
    return text_html, button_url

# ---------------- Telegram send ----------------
def tg_send_card(text_html: str, button_url: str):
    url = TG_SEND.format(token=TG_BOT_TOKEN)
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": json.dumps({
            "inline_keyboard": [[{"text": "📄 DART 열기", "url": button_url}]]
        }, ensure_ascii=False)
    }
    r = S.post(url, data=payload, timeout=30)
    r.raise_for_status()

# ---------------- MAIN ----------------
def main():
    must_env()

    st = load_state()
    sent = 0

    items = fetch_disclosures_paginated()

    # 오래된 것부터 처리(알림 순서 안정)
    items = list(reversed(items))

    # 1) 1차 필터링 + seen 제거
    filtered: List[Dict[str, Any]] = []
    for it in items:
        rcept_no = _clean(it.get("rcept_no", ""))
        if not rcept_no:
            continue
        if is_seen(st, rcept_no):
            continue

        rpt_nm = it.get("report_nm", "")
        if not match_report_name(rpt_nm):
            continue

        # 시장 구분: list.json 에 corp_cls / corp_class 등이 있을 수 있어 우선 corp_cls 사용, 없으면 빈값
        market = _clean(it.get("corp_cls") or it.get("corp_class") or "")
        if market and not market_ok(market):
            continue

        filtered.append(it)

    # 2) 상세 조회(유상증자만) + 제3자배정 상세 차단 + 조달금액 필터
    ready: List[Dict[str, Any]] = []
    for it in filtered:
        rpt_nm = _clean(it.get("report_nm", ""))
        corp_code = _clean(it.get("corp_code", ""))
        rcept_dt = _clean(it.get("rcept_dt", ""))
        rcept_no = _clean(it.get("rcept_no", ""))

        detail = None
        if "유상" in rpt_nm and corp_code and rcept_dt:
            detail = fetch_pifric_detail(corp_code, rcept_dt, rcept_no)
            if detail:
                # 상세에서도 제3자배정이면 제외
                if detect_third_party_from_detail(detail):
                    mark_seen(st, rcept_no)
                    continue
                # 조달금액 필터(가능할 때만)
                raise_amt = extract_raise_amount(detail)
                if MIN_RAISE_KRW > 0 and raise_amt > 0 and raise_amt < MIN_RAISE_KRW:
                    mark_seen(st, rcept_no)
                    continue

        it["_detail"] = detail or {}
        ready.append(it)

    # 3) 같은 회사/같은 접수일로 묶어서 1건 발송
    groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
    for it in ready:
        corp = _clean(it.get("corp_name", "N/A"))
        market = _clean(it.get("corp_cls") or it.get("corp_class") or "")
        rcept_dt = _clean(it.get("rcept_dt", ""))
        key = (corp, market or "N/A", rcept_dt or "N/A")
        groups.setdefault(key, []).append(it)

    # 4) 발송
    for key, grp in groups.items():
        # 그룹 내에서 “유상→무상” 같이 섞일 수 있으니 정렬
        grp = sorted(grp, key=lambda x: _clean(x.get("report_nm","")))
        text_html, button_url = build_card_html(key, grp)

        # 최소한 텍스트가 있어야 전송
        if not text_html.strip():
            for it in grp:
                mark_seen(st, _clean(it.get("rcept_no","")))
            continue

        tg_send_card(text_html, button_url)

        for it in grp:
            mark_seen(st, _clean(it.get("rcept_no","")))
        sent += 1

    save_state(st)
    print(f"OK sent={sent} seen={len(st.get('seen', []))}")

if __name__ == "__main__":
    main()
