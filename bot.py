#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DART Rights Issue / Bonus Issue Telegram Alert (Ops-grade, GitHub Actions friendly)

✅ 포함:
- 일반공모/주주배정 유상증자 + 무상증자 + 유무상증자(주주배정/일반) (DART list 기반 + 상세 API 보강)
- (A) page_no 페이지네이션으로 누락 방지
- (B) 키워드/정규식 매칭 정확도 개선
- (C) 같은 회사/같은 날짜 연속 공시 묶어서 1건으로 발송
- (D) 텔레그램 카드형(HTML) + 인라인 버튼(📄 DART 열기)
- N/A 자동 숨김
- 위험도 점수(조달금액/시총비율, 할인율, 최대주주 참여 여부 가산/감산) 표시
- state.json에 seen 저장(=서버리스에서도 중복 방지). GitHub Actions에서 commit state.json 지원.

⚠️ 제3자배정 유상증자(제3자배정증자 등) "제외" 기본 동작.
"""

import os
import re
import json
import math
import time
import html
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

# =========================
# ENV
# =========================
DART_API_KEY = os.getenv("DART_API_KEY", "").strip()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "3"))                 # 며칠치 훑기
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))                  # 로컬 루프용(서버리스는 1회 실행)
RUN_ONCE = os.getenv("RUN_ONCE", "true").strip().lower() == "true"   # GitHub Actions 기본은 true(스케줄로 반복)

# 시장구분 필터: K=KOSPI, Q=KOSDAQ, N=KONEX (DART list의 corp_cls)
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES", "K,Q,N").split(",") if x.strip()]

# 조달금액(원) 최소 필터 (0이면 필터 안함)
MIN_RAISE_KRW = int(os.getenv("MIN_RAISE_KRW", "0").strip() or "0")

# state.json
STATE_PATH = os.getenv("STATE_PATH", "state.json")

# =========================
# DART API
# =========================
LIST_URL = "https://opendart.fss.or.kr/api/list.json"
PIFRIC_URL = "https://opendart.fss.or.kr/api/pifricDecsn.json"   # 유상증자결정 상세(주요사항보고서)
ALLOT_URL = "https://opendart.fss.or.kr/api/alotMatter.json"     # 무상증자결정 상세(주요사항보고서)
# (참고) DART 뷰어
DART_VIEWER = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

# =========================
# Telegram
# =========================
TG_SEND = "https://api.telegram.org/bot{token}/sendMessage"

S = requests.Session()
S.headers.update({"User-Agent": "dart-alert-actions/1.1"})


def must_env():
    missing = []
    if not DART_API_KEY:
        missing.append("DART_API_KEY")
    if not TG_BOT_TOKEN:
        missing.append("TG_BOT_TOKEN")
    if not TG_CHAT_ID:
        missing.append("TG_CHAT_ID")
    if missing:
        raise SystemExit(f"[ERROR] Missing env: {', '.join(missing)}")


# =========================
# State (seen)
# =========================
def load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            st = json.load(f)
        if "seen" not in st or not isinstance(st["seen"], list):
            st["seen"] = []
        return st
    except Exception:
        return {"seen": []}


def save_state(st: Dict[str, Any]) -> None:
    tmp = json.dumps(st, ensure_ascii=False, indent=2)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        f.write(tmp + "\n")


def is_seen(st: Dict[str, Any], rcept_no: str) -> bool:
    return rcept_no in set(st.get("seen", []))


def mark_seen(st: Dict[str, Any], rcept_no: str) -> None:
    if "seen" not in st or not isinstance(st["seen"], list):
        st["seen"] = []
    if rcept_no not in st["seen"]:
        st["seen"].append(rcept_no)
        # 너무 커지지 않게 최근 5000개만 유지
        if len(st["seen"]) > 5000:
            st["seen"] = st["seen"][-5000:]


# =========================
# Helpers
# =========================
def _to_int(x) -> int:
    if x is None:
        return 0
    s = str(x).strip()
    if not s:
        return 0
    try:
        return int(s.replace(",", ""))
    except Exception:
        return 0


def _fmt_int(v: int) -> str:
    return f"{v:,}" if v else ""


def _fmt_date_yyyymmdd(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # DART 종종 YYYYMMDD / YYYY-MM-DD 혼재
    if re.fullmatch(r"\d{8}", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return s


def _safe(s: str) -> str:
    return html.escape(s or "")


def _pick_market(corp_cls: str) -> str:
    # DART corp_cls: Y=유가, K=코스닥, N=코넥스, E=기타? (현장에서는 K/Q/N로 쓰는 경우도 있어 혼재)
    # 실제 list.json은 'corp_cls' 가 Y,K,N 로 내려오는 경우가 흔함.
    m = (corp_cls or "").strip().upper()
    if m == "Y":
        return "KOSPI"
    if m == "K":
        return "KOSDAQ"
    if m == "N":
        return "KONEX"
    if m == "Q":
        return "KOSDAQ"
    return m or "N/A"


# =========================
# Matching (include shareholder/general, exclude 3rd party)
# =========================
# 포함 키워드: 유상증자/무상증자/유무상증자 + 결정/주요사항보고서
INC_RE = re.compile(
    r"(유상증자|무상증자|유무상증자)"
    r".*(결정|주요사항보고서)",
    re.IGNORECASE
)

# 제외 키워드: 제3자배정 (보고서명 또는 상세의 '증자방식'에 흔히 등장)
EXC_RE = re.compile(r"(제\s*3\s*자|제3자)\s*배정", re.IGNORECASE)

# 일반/주주배정(또는 일반공모) 포함 키워드(가능한 넓게)
ALLOW_METHOD_RE = re.compile(r"(일반공모|일반\s*공모|주주배정|주주\s*배정|일반주주)", re.IGNORECASE)


def match_report_name(report_nm: str) -> bool:
    rn = (report_nm or "").strip()
    if not rn:
        return False
    if not INC_RE.search(rn):
        return False
    # 보고서명에 제3자배정이 박혀 있으면 즉시 제외
    if EXC_RE.search(rn):
        return False
    return True


# =========================
# DART list pagination
# =========================
def fetch_disclosures_all() -> List[Dict[str, Any]]:
    end_de = datetime.now().strftime("%Y%m%d")
    bgn_de = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    page_no = 1
    page_count = 100
    out: List[Dict[str, Any]] = []

    while True:
        params = {
            "crtfc_key": DART_API_KEY,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "pblntf_ty": "B",  # 주요사항보고
            "page_no": page_no,
            "page_count": page_count,
            "sort": "date",
            "sort_mth": "desc",
        }
        r = S.get(LIST_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        status = data.get("status")
        if status == "000":
            items = data.get("list", []) or []
            if not items:
                break
            out.extend(items)
            # 더 이상 페이지가 없으면 종료
            total_count = _to_int(data.get("total_count"))
            if total_count and len(out) >= total_count:
                break
            # 혹시 total_count 없으면 길이로 추정
            if len(items) < page_count:
                break
            page_no += 1
            continue
        if status == "013":
            break

        raise RuntimeError(f"DART list error: {status} / {data.get('message','')}")
    return out


# =========================
# Detail fetchers
# =========================
def fetch_pifric_detail(corp_code: str, rcept_dt: str, rcept_no: str) -> Optional[Dict[str, Any]]:
    # 유상증자결정(주요사항보고서)
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
    if status == "000":
        for it in (data.get("list", []) or []):
            if (it.get("rcept_no") or "").strip() == rcept_no:
                return it
        return None
    if status == "013":
        return None
    raise RuntimeError(f"PIFRIC error: {status} / {data.get('message','')}")


def fetch_allot_detail(corp_code: str, rcept_dt: str, rcept_no: str) -> Optional[Dict[str, Any]]:
    # 무상증자결정(주요사항보고서)
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": rcept_dt,
        "end_de": rcept_dt,
    }
    r = S.get(ALLOT_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    status = data.get("status")
    if status == "000":
        for it in (data.get("list", []) or []):
            if (it.get("rcept_no") or "").strip() == rcept_no:
                return it
        return None
    if status == "013":
        return None
    raise RuntimeError(f"ALOT error: {status} / {data.get('message','')}")


# =========================
# Normalizers: extract fields for message / scoring
# =========================
def extract_raise_purpose_from_pifric(d: Dict[str, Any]) -> Tuple[str, int]:
    # 목적별 + 합계
    fclt = _to_int(d.get("piic_fdpp_fclt"))
    op = _to_int(d.get("piic_fdpp_op"))
    dtrp = _to_int(d.get("piic_fdpp_dtrp"))
    ocsa = _to_int(d.get("piic_fdpp_ocsa"))
    etc = _to_int(d.get("piic_fdpp_etc"))
    total = fclt + op + dtrp + ocsa + etc

    parts = []
    if fclt: parts.append(f"시설 {_fmt_int(fclt)}원")
    if op: parts.append(f"운영 {_fmt_int(op)}원")
    if dtrp: parts.append(f"채무상환 {_fmt_int(dtrp)}원")
    if ocsa: parts.append(f"타법인증권취득 {_fmt_int(ocsa)}원")
    if etc: parts.append(f"기타 {_fmt_int(etc)}원")

    return (", ".join(parts), total)


def extract_schedule_fields_pifric(d: Dict[str, Any]) -> Dict[str, str]:
    # DART 필드명은 케이스/버전에 따라 다를 수 있어 넓게 시도
    def g(*keys: str) -> str:
        for k in keys:
            v = (d.get(k) or "").strip()
            if v:
                return v
        return ""

    out = {
        "신주배정기준일": _fmt_date_yyyymmdd(g("piic_nstk_asstd", "nstk_asstd", "asstd")),
        "예정가": g("piic_exrt", "piic_nstk_prc", "nstk_prc", "exrt"),
        "확정일": _fmt_date_yyyymmdd(g("piic_prc_dcsn_de", "prc_dcsn_de", "dcsn_de")),
        "신주인수권상장예정기간": g("piic_nstk_rts_lstg_pd", "nstk_rts_lstg_pd", "rts_lstg_pd"),
        "청약일": g("piic_sbc_de", "sbc_de"),
        "신주의상장예정일": _fmt_date_yyyymmdd(g("piic_nstk_lstg_de", "nstk_lstg_de", "lstg_de")),
        "증자방식": (d.get("piic_ic_mthn") or "").strip(),
        "1주당신주배정주식수": g("piic_nstk_asst_ps", "nstk_asst_ps"),
        "최대주주참여": g("piic_mxmm_shh_ptcptn_at", "mxmm_shh_ptcptn_at", "mxmm_shh_yn"),
    }
    return out


def extract_bonus_fields_allot(d: Dict[str, Any]) -> Dict[str, str]:
    def g(*keys: str) -> str:
        for k in keys:
            v = (d.get(k) or "").strip()
            if v:
                return v
        return ""

    out = {
        "무상신주배정기준일": _fmt_date_yyyymmdd(g("nstk_asstd", "asstd")),
        "1주당무상배정주식수": g("nstk_asst_ps", "asst_ps"),
        "신주의상장예정일": _fmt_date_yyyymmdd(g("nstk_lstg_de", "lstg_de")),
        "무상증자재원": g("nstk_issu_frm", "issu_frm"),
    }
    return out


def is_third_party_excluded(report_nm: str, detail_method: str) -> bool:
    # report name or method contains 3rd party
    if EXC_RE.search(report_nm or ""):
        return True
    if EXC_RE.search(detail_method or ""):
        return True
    return False


def is_allowed_method(detail_method: str) -> bool:
    # 상세의 증자방식이 비어있으면 report_name 기반으로만 통과(너무 누락 방지)
    m = (detail_method or "").strip()
    if not m:
        return True
    if EXC_RE.search(m):
        return False
    # 일반공모/주주배정/일반주주 포함만 허용 (요구사항)
    return ALLOW_METHOD_RE.search(m) is not None


# =========================
# Risk score (0~100)
# =========================
def compute_risk_score(
    raise_krw: int,
    mcap_krw: int,
    planned_price: float,
    current_price: float,
    major_sh_holder: str
) -> Tuple[int, List[str]]:
    """
    위험도 = 투자 위험도(희석/디스카운트/조달 규모 중심) 간이 점수
    - 시총대비 조달비율 높을수록 위험↑
    - 할인율(예정가 vs 현재가) 클수록 위험↑
    - 최대주주 참여 'Y/예/참여'면 위험↓, 'N/아니오/불참'면 위험↑
    """
    reasons = []
    score = 0

    # 시총대비 조달비율
    ratio = 0.0
    if raise_krw > 0 and mcap_krw > 0:
        ratio = raise_krw / mcap_krw
        # 5% -> +10, 10% -> +20, 20% -> +35, 30% -> +45, 50% -> +60
        if ratio >= 0.5:
            score += 60
        elif ratio >= 0.3:
            score += 45
        elif ratio >= 0.2:
            score += 35
        elif ratio >= 0.1:
            score += 20
        elif ratio >= 0.05:
            score += 10
        reasons.append(f"조달/시총 {ratio*100:.1f}%")

    # 할인율
    disc = 0.0
    if planned_price > 0 and current_price > 0:
        disc = max(0.0, (current_price - planned_price) / current_price)
        # 5% -> +5, 10% -> +12, 20% -> +25, 30% -> +35, 40% -> +45
        if disc >= 0.4:
            score += 45
        elif disc >= 0.3:
            score += 35
        elif disc >= 0.2:
            score += 25
        elif disc >= 0.1:
            score += 12
        elif disc >= 0.05:
            score += 5
        reasons.append(f"할인율 {disc*100:.1f}%")

    # 최대주주 참여
    msh = (major_sh_holder or "").strip().lower()
    if msh:
        if msh in ("y", "yes", "예", "참여", "참여함", "있음", "o", "true"):
            score -= 8
            reasons.append("최대주주 참여(+안정)")
        elif msh in ("n", "no", "아니오", "불참", "없음", "x", "false"):
            score += 10
            reasons.append("최대주주 불참(+위험)")

    score = max(0, min(100, score))
    return score, reasons


# =========================
# Telegram send (card-like HTML + button)
# =========================
def tg_send_card(title: str, body_lines: List[str], button_url: str) -> None:
    url = TG_SEND.format(token=TG_BOT_TOKEN)

    # N/A/빈 줄 자동 숨김
    body_lines = [ln for ln in body_lines if ln and ln.strip() and "N/A" not in ln]

    text = "<b>" + _safe(title) + "</b>\n" + "\n".join(body_lines)
    # 텔레그램 메시지 길이 제한(4096) 대응: 안전하게 컷
    if len(text) > 3900:
        text = text[:3900] + "\n…"

    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": json.dumps({
            "inline_keyboard": [[{"text": "📄 DART 열기", "url": button_url}]]
        }, ensure_ascii=False),
    }
    resp = S.post(url, data=payload, timeout=25)
    resp.raise_for_status()


# =========================
# Grouping (same corp + same date)
# =========================
def group_items(items: List[Dict[str, Any]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for it in items:
        corp = (it.get("corp_name") or "").strip()
        dt = (it.get("rcept_dt") or "").strip()
        if not corp or not dt:
            continue
        key = (corp, dt)
        groups.setdefault(key, []).append(it)
    return groups


# =========================
# Main processing
# =========================
def process_once() -> int:
    st = load_state()

    all_items = fetch_disclosures_all()

    # 최신순으로 오지만, 전송은 오래된 것부터 보내기 위해 역순
    all_items = list(reversed(all_items))

    # 1) 1차: report name 필터 + market 필터 + seen 필터
    cand: List[Dict[str, Any]] = []
    for it in all_items:
        rpt_nm = it.get("report_nm", "")
        if not match_report_name(rpt_nm):
            continue

        corp_cls = (it.get("corp_cls") or "").strip().upper()
        # MARKET_CLASSES는 K/Q/N로 받지만 DART는 Y/K/N일 수 있음
        # 여기서는 KOSPI=Y도 허용되도록 매핑해서 비교
        if MARKET_CLASSES:
            # 허용 목록을 DART corp_cls 기준으로 확장
            allow = set()
            for mc in MARKET_CLASSES:
                if mc == "K":
                    allow.add("Y")
                elif mc == "Q":
                    allow.add("K")
                else:
                    allow.add(mc)
            if corp_cls and corp_cls not in allow:
                continue

        rcept_no = (it.get("rcept_no") or "").strip()
        if not rcept_no or is_seen(st, rcept_no):
            continue

        cand.append(it)

    if not cand:
        print("OK sent=0 seen=%d" % len(st.get("seen", [])))
        return 0

    # 2) 그룹핑(같은 회사+같은 접수일)
    grouped = group_items(cand)

    sent = 0
    for (corp, rcept_dt), group in grouped.items():
        # group 내부는 여러 공시(정정 포함) 있을 수 있음 → 제목 리스트로 정리
        # 또한 상세로 제3자배정이면 그룹 전체에서 그 항목 제외, 남는게 없으면 skip
        details_for_lines: List[str] = []
        any_url = ""
        market_str = ""

        # risk 계산용(가능하면)
        raise_total = 0
        mcap = 0
        planned_price = 0.0
        current_price = 0.0
        major_sh = ""

        accepted_any = False

        # 각 항목 처리
        for it in group:
            rpt_nm = (it.get("report_nm") or "").strip()
            rcept_no = (it.get("rcept_no") or "").strip()
            corp_code = (it.get("corp_code") or "").strip()
            corp_cls = (it.get("corp_cls") or "").strip()
            market_str = _pick_market(corp_cls)
            url = DART_VIEWER.format(rcept_no=rcept_no)
            if not any_url:
                any_url = url

            # 유상/무상 상세 조회로 증자방식/일정 추출
            method = ""
            schedule_lines: List[str] = []
            purpose_str = ""
            purpose_total = 0

            try:
                # 유상(또는 유무상 중 유상 파트)
                pif = fetch_pifric_detail(corp_code, rcept_dt, rcept_no) if (corp_code and rcept_dt) else None
                if pif:
                    method = (pif.get("piic_ic_mthn") or "").strip()
                    # 3자배정 제외
                    if is_third_party_excluded(rpt_nm, method):
                        mark_seen(st, rcept_no)
                        continue
                    # 일반/주주배정만 허용(요구사항)
                    if not is_allowed_method(method):
                        mark_seen(st, rcept_no)
                        continue

                    sched = extract_schedule_fields_pifric(pif)
                    # 목적/조달금액
                    purpose_str, purpose_total = extract_raise_purpose_from_pifric(pif)

                    # 조달금액 필터
                    if MIN_RAISE_KRW > 0 and purpose_total > 0 and purpose_total < MIN_RAISE_KRW:
                        mark_seen(st, rcept_no)
                        continue

                    # risk 계산용 값들 (필드명은 케이스별로 다를 수 있어 넓게)
                    # 예정가
                    try:
                        planned_price = float(str(sched.get("예정가") or "").replace(",", ""))
                    except Exception:
                        pass
                    # 현재가/시총은 DART API에서 안정적으로 안 나오는 경우가 많아,
                    # 여기서는 값이 없으면 스킵(점수 계산은 가능한 범위만)
                    # (향후 KRX/네이버 금융 API 붙일 수 있음)
                    major_sh = sched.get("최대주주참여") or major_sh

                    raise_total = max(raise_total, purpose_total)

                    # 라인 구성
                    if method:
                        schedule_lines.append(f"• 증자방식: <b>{_safe(method)}</b>")
                    if purpose_str:
                        schedule_lines.append(f"• 자금조달목적: {_safe(purpose_str)}")
                        schedule_lines.append(f"• 조달합계: <b>{_safe(_fmt_int(purpose_total))}원</b>")

                    # 일정들(N/A 숨김은 아래에서 일괄)
                    for k in ["신주배정기준일", "예정가", "확정일", "신주인수권상장예정기간", "청약일", "신주의상장예정일", "1주당신주배정주식수"]:
                        v = (sched.get(k) or "").strip()
                        if v:
                            schedule_lines.append(f"• {k}: {_safe(v)}")

                    accepted_any = True

                else:
                    # 무상증자 상세
                    alt = fetch_allot_detail(corp_code, rcept_dt, rcept_no) if (corp_code and rcept_dt) else None
                    if alt:
                        # 무상은 제3자배정 개념이 거의 없지만, 혹시 report에 들어가면 제외
                        if EXC_RE.search(rpt_nm):
                            mark_seen(st, rcept_no)
                            continue

                        bonus = extract_bonus_fields_allot(alt)
                        # 무상도 일반/주주배정 개념이 약하지만, 요구: "일반주주/주주배정 무상" 포함 → 그냥 무상은 포함
                        schedule_lines.append("• 증자방식: <b>무상증자</b>")
                        for k in ["무상신주배정기준일", "1주당무상배정주식수", "신주의상장예정일", "무상증자재원"]:
                            v = (bonus.get(k) or "").strip()
                            if v:
                                schedule_lines.append(f"• {k}: {_safe(v)}")
                        accepted_any = True
                    else:
                        # 상세 못 땡겨도 report 명이 포함조건이면 기본 알림만(단, 제3자배정 명시돼 있으면 제외는 이미 됨)
                        accepted_any = True

            except Exception as e:
                # 상세 실패해도 기본 알림은 보내되, 제3자배정은 report 명으로라도 제외됨
                schedule_lines.append(f"• (상세조회 실패: {_safe(str(e))})")
                accepted_any = True

            # 공시명 라인
            if accepted_any:
                details_for_lines.append(f"• {_safe(_fmt_date_yyyymmdd(rcept_dt))} – {_safe(rpt_nm)} ({_safe(rcept_no)})")
                if schedule_lines:
                    # 공시별 블록 구분
                    details_for_lines.extend(schedule_lines)
                    details_for_lines.append("")  # 빈줄

            # seen 처리 (성공/스킵 포함해 처리된 rcept_no는 seen에 넣어 중복 방지)
            mark_seen(st, rcept_no)

        # 그룹 내 유효 항목이 없으면 스킵(모두 3자/비허용 방식 등)
        if not accepted_any or not details_for_lines:
            continue

        # 위험도 점수
        risk_score, risk_reasons = compute_risk_score(
            raise_krw=raise_total,
            mcap_krw=mcap,
            planned_price=planned_price,
            current_price=current_price,
            major_sh_holder=major_sh
        )
        risk_line = ""
        if risk_reasons:
            risk_line = f"• 위험도: <b>{risk_score}/100</b> (" + ", ".join(_safe(x) for x in risk_reasons) + ")"

        title = f"📌 증자 공시 감지"
        header_lines = [
            f"• 회사: <b>{_safe(corp)}</b> ({_safe(market_str)})",
            f"• 접수일: <b>{_safe(_fmt_date_yyyymmdd(rcept_dt))}</b>",
        ]
        if risk_line:
            header_lines.append(risk_line)
        header_lines.append("")
        header_lines.append("<b>공시</b>")
        header_lines.extend(details_for_lines)

        tg_send_card(title=title, body_lines=header_lines, button_url=any_url or "https://dart.fss.or.kr")
        sent += 1

    save_state(st)
    print(f"OK sent={sent} seen={len(st.get('seen', []))}")
    return sent


def main():
    must_env()
    print("[START] dart-alert bot.py")
    print(f"  LOOKBACK_DAYS={LOOKBACK_DAYS} RUN_ONCE={RUN_ONCE} MARKET_CLASSES={MARKET_CLASSES} MIN_RAISE_KRW={MIN_RAISE_KRW}")

    if RUN_ONCE:
        process_once()
        return

    while True:
        try:
            process_once()
        except Exception as e:
            print("[ERROR]", repr(e))
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
