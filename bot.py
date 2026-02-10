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

# 네이버 금융 조회 타임아웃/재시도
NAVER_TIMEOUT = int(os.getenv("NAVER_TIMEOUT", "20"))
NAVER_RETRY   = int(os.getenv("NAVER_RETRY", "2"))

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

NAVER_ITEM = "https://finance.naver.com/item/main.nhn?code={}"

S = requests.Session()
S.headers.update({"User-Agent": "dart-alert-github-actions/6.0"})

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
# Money parsing / Raise amount (heuristic)
# =========================
def parse_int_kr(s: str) -> int:
    if not s:
        return 0
    s = re.sub(r"[^\d,]", "", s)
    if not s:
        return 0
    try:
        return int(s.replace(",", ""))
    except Exception:
        return 0

def fmt_won(n: int) -> str:
    if n <= 0:
        return "N/A"
    return f"{n:,}원"

def extract_raise_amount_krw(doc_text: str) -> int:
    """
    조달금액(추정):
    - '모집', '매출', '금액' 근처의 'xxx원' 우선
    - 없으면 문서 내 가장 큰 '원' 금액(상식적 상한 적용)
    """
    if not doc_text:
        return 0

    lines = [ln.strip() for ln in doc_text.splitlines() if ln.strip()]
    amt_pat = re.compile(r"(\d{1,3}(?:,\d{3})+|\d+)\s*원")
    focus_kw = re.compile(r"(모집|매출|발행|조달|자금|총액|금액)", re.I)

    candidates = []
    for ln in lines:
        if not focus_kw.search(ln):
            continue
        for m in amt_pat.finditer(ln):
            v = parse_int_kr(m.group(1))
            if v > 0:
                candidates.append(v)

    # 1) 키워드 라인 후보가 있으면 그중 최대를 사용
    if candidates:
        v = max(candidates)
        # 비정상 초대형 방지(100조 이상은 보통 오탐) - 필요시 조정
        if v >= 100_000_000_000_000:
            return 0
        return v

    # 2) fallback: 문서 전체에서 합리적 범위 내 최대 '원' 금액
    all_vals = []
    for ln in lines:
        for m in amt_pat.finditer(ln):
            v = parse_int_kr(m.group(1))
            if v > 0:
                all_vals.append(v)
    if not all_vals:
        return 0

    v = max(all_vals)
    if v >= 100_000_000_000_000:
        return 0
    return v

# =========================
# Naver finance: current price + market cap
# =========================
def fetch_naver_html(stock_code: str) -> str:
    url = NAVER_ITEM.format(stock_code)
    last = None
    for _ in range(max(1, NAVER_RETRY)):
        try:
            r = S.get(url, timeout=NAVER_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
    raise RuntimeError(f"Naver fetch failed: {last}")

def extract_current_price_from_naver(ht: str) -> int:
    # 현재가 영역(여러 패턴 대응)
    # 예) <p class="no_today"><span class="blind">12,340</span>
    m = re.search(r'no_today[^>]*>\s*<[^>]*>\s*<span[^>]*class="blind"[^>]*>([\d,]+)</span>', ht, re.I | re.S)
    if m:
        return parse_int_kr(m.group(1))

    # fallback: "현재가" 텍스트 근처
    m = re.search(r"(현재가|종가)[^0-9]{0,30}([\d,]+)", ht)
    if m:
        return parse_int_kr(m.group(2))
    return 0

def extract_market_cap_from_naver(ht: str) -> int:
    """
    네이버는 시가총액을 '시가총액' 라벨 근처에 숫자로 노출.
    단위가 '억원'로 보이는 경우가 있어 변환 처리.
    """
    # 1) "시가총액" 근처에서 숫자+단위(억원/조/원 등) 탐지
    # 예: 시가총액 1조 2,345억 / 또는 12,345억원
    m = re.search(r"시가총액\s*</th>\s*<td[^>]*>\s*([^<]+)</td>", ht, re.I | re.S)
    if m:
        raw = html.unescape(m.group(1))
        raw = re.sub(r"\s+", " ", raw).strip()

        # 형태: "12조 3,456억" 처리
        tj = re.search(r"(\d{1,3}(?:,\d{3})+|\d+)\s*조", raw)
        ek = re.search(r"(\d{1,3}(?:,\d{3})+|\d+)\s*억", raw)
        if tj or ek:
            val = 0
            if tj:
                val += parse_int_kr(tj.group(1)) * 1_0000_0000_0000  # 1조 = 1e12
            if ek:
                val += parse_int_kr(ek.group(1)) * 100_000_000        # 1억 = 1e8
            return val if val > 0 else 0

        # 형태: "12,345억원" 처리
        m2 = re.search(r"([\d,]+)\s*억", raw)
        if m2:
            return parse_int_kr(m2.group(1)) * 100_000_000

        # 형태: "123,456,789,000원" 처리
        m3 = re.search(r"([\d,]+)\s*원", raw)
        if m3:
            return parse_int_kr(m3.group(1))

    # 2) fallback: 본문에서 '시가총액' 다음 숫자/단위 탐지
    m = re.search(r"시가총액[^0-9]{0,80}([\d,]+)\s*억", ht)
    if m:
        return parse_int_kr(m.group(1)) * 100_000_000

    return 0

def get_price_and_mcap(stock_code: str) -> tuple[int, int]:
    if not stock_code or not re.fullmatch(r"\d{6}", stock_code):
        return 0, 0
    ht = fetch_naver_html(stock_code)
    px = extract_current_price_from_naver(ht)
    mc = extract_market_cap_from_naver(ht)
    return px, mc

# =========================
# 최대주주 참여 여부(휴리스틱)
# =========================
def extract_major_shareholder_participation(doc_text: str) -> tuple[str, str]:
    """
    참여/불참/미확인 + 근거 문구 일부 반환
    """
    if not doc_text:
        return "미확인", ""

    lines = [ln.strip() for ln in doc_text.splitlines() if ln.strip()]
    # 최대주주/특수관계인/최대주주의 청약/참여/인수 등
    pat = re.compile(r"(최대\s*주주|최대주주|특수관계인|대주주).{0,60}(참여|청약|인수|불참|미참여|포기)", re.I)
    neg = re.compile(r"(불참|미참여|포기)", re.I)
    pos = re.compile(r"(참여|청약|인수)", re.I)

    for ln in lines:
        if "최대" not in ln and "대주주" not in ln and "특수" not in ln:
            continue
        m = pat.search(ln)
        if not m:
            continue
        snippet = _norm_ws(ln)[:180]
        if neg.search(ln):
            return "불참", snippet
        if pos.search(ln):
            return "참여", snippet

    # fallback: '최대주주' 문맥 1~2줄 합쳐서 판단
    for i, ln in enumerate(lines):
        if re.search(r"(최대\s*주주|최대주주|대주주|특수관계인)", ln):
            ctx = ln
            if i + 1 < len(lines):
                ctx2 = _norm_ws(lines[i + 1])
                if ctx2:
                    ctx = _norm_ws(ctx + " " + ctx2)
            if neg.search(ctx):
                return "불참", ctx[:180]
            if pos.search(ctx):
                return "참여", ctx[:180]

    return "미확인", ""

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

def fmt_pct(x: float) -> str:
    return f"{x:.2f}%"

def risk_score(ev_type: str, alloc: str, market: str, doc_text: str, fields: dict,
               raise_krw: int, mcap_krw: int, discount_pct: float | None,
               major_part: str) -> tuple[int, str, str]:
    """
    0~100 휴리스틱(업그레이드):
    - 기존(유형/시장/목적/일정) + 시총대비 비율 + 할인율 + 최대주주 불참 가중
    """
    score = 10

    et = (ev_type or "").strip()
    if et == "유상":
        score += 38
    elif et == "유무상":
        score += 28
    elif et == "무상":
        score += 8
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

    if alloc == "주주배정":
        score += 8
    elif alloc == "일반주주배정":
        score += 6

    purpose = (fields.get("자금조달의목적") or "")
    if re.search(r"(채무|상환|차입|대출)", purpose):
        score += 18
    if re.search(r"(운영|운전자금)", purpose):
        score += 10
    if re.search(r"(타법인|M&A|인수|취득|투자)", purpose):
        score += 12

    for k in ["청약일", "예정가", "확정일", "신주인수권상장예정기간", "신주의상장예정일"]:
        if not _is_empty_value(fields.get(k, "N/A")):
            score += 4

    # ✅ 시총 대비 조달금액 비율 반영
    if raise_krw > 0 and mcap_krw > 0:
        ratio = raise_krw / mcap_krw  # 0~1+
        if ratio >= 0.50:
            score += 22
        elif ratio >= 0.30:
            score += 16
        elif ratio >= 0.15:
            score += 10
        elif ratio >= 0.05:
            score += 6
        else:
            score += 2

    # ✅ 할인율 반영(예정가가 현재가 대비 크게 낮으면 이벤트 영향↑)
    if discount_pct is not None:
        if discount_pct >= 35:
            score += 16
        elif discount_pct >= 25:
            score += 12
        elif discount_pct >= 15:
            score += 8
        elif discount_pct >= 8:
            score += 4

    # ✅ 최대주주 참여(불참이면 가중)
    if major_part == "불참":
        score += 18
    elif major_part == "참여":
        score -= 6  # 참여는 리스크 완화 신호로 약하게 감점

    # clamp
    score = max(0, min(100, score))

    if score >= 80:
        emoji, grade = "🔴", "높음"
    elif score >= 60:
        emoji, grade = "🟠", "중간"
    elif score >= 40:
        emoji, grade = "🟡", "낮음"
    else:
        emoji, grade = "🟢", "매우낮음"
    return score, grade, emoji

def build_card(corp: str, market: str, ev_type: str, alloc: str, rcept_dt: str, rpt_nm: str, url: str,
               doc_text: str, fields: dict, stock_code: str | None,
               cur_px: int, mcap_krw: int, raise_krw: int,
               ratio_pct: float | None, discount_pct: float | None,
               major_part: str, major_snip: str) -> str:

    score, grade, emoji = risk_score(
        ev_type, alloc, market, doc_text, fields,
        raise_krw=raise_krw, mcap_krw=mcap_krw, discount_pct=discount_pct,
        major_part=major_part
    )

    lines = []
    lines.append(f"{emoji} <b>증자 공시 감지</b>  <i>(위험도 {score}/100 · {grade})</i>")
    lines.append(f"🏢 <b>{html.escape(corp)}</b>  <i>({html.escape(market)})</i>")
    lines.append(f"🧾 유형: <b>{html.escape(ev_type)}</b> / 배정: <b>{html.escape(alloc)}</b>")
    if rcept_dt:
        lines.append(f"📅 접수일: {html.escape(rcept_dt)}")
    if stock_code and re.fullmatch(r"\d{6}", stock_code):
        lines.append(f"🔎 종목코드: {html.escape(stock_code)}")

    lines.append("────────────────────")
    lines.append(f"📌 <b>공시명</b>")
    lines.append(f"{html.escape(rpt_nm)}")
    lines.append("────────────────────")

    # 🧠 핵심
    core = []
    add_if(core, "자금조달의목적", fields.get("자금조달의목적", "N/A"))
    add_if(core, "신주배정기준일", fields.get("신주배정기준일", "N/A"))
    if core:
        lines.append("🧠 <b>핵심</b>")
        lines.extend(core)
        lines.append("────────────────────")

    # 💰 가격
    price = []
    add_if(price, "예정가", fields.get("예정가", "N/A"))
    add_if(price, "확정일", fields.get("확정일", "N/A"))
    if price:
        lines.append("💰 <b>가격</b>")
        lines.extend(price)
        lines.append("────────────────────")

    # 🗓️ 일정
    sched = []
    add_if(sched, "신주인수권상장예정기간", fields.get("신주인수권상장예정기간", "N/A"))
    add_if(sched, "청약일", fields.get("청약일", "N/A"))
    add_if(sched, "신주의상장예정일", fields.get("신주의상장예정일", "N/A"))
    if sched:
        lines.append("🗓️ <b>일정</b>")
        lines.extend(sched)
        lines.append("────────────────────")

    # 📊 규모/비율/할인
    size = []
    if raise_krw > 0:
        add_if(size, "조달금액(추정)", fmt_won(raise_krw))
    if mcap_krw > 0:
        add_if(size, "시가총액(추정)", fmt_won(mcap_krw))
    if ratio_pct is not None:
        add_if(size, "시총 대비 조달비율", fmt_pct(ratio_pct))
    if cur_px > 0:
        add_if(size, "현재가(추정)", f"{cur_px:,}원")
    if discount_pct is not None:
        add_if(size, "예정가 할인율", fmt_pct(discount_pct))
    if major_part != "미확인":
        add_if(size, "최대주주 참여", major_part)
        if major_snip:
            add_if(size, "근거", major_snip)

    if size:
        lines.append("📊 <b>규모/리스크 보강</b>")
        lines.extend(size)
        lines.append("────────────────────")

    lines.append("➡️ 아래 버튼으로 원문 확인")
    return "\n".join(lines)

# =========================
# helpers: 예정가 숫자 추출
# =========================
def extract_issue_price_from_field(v: str) -> int:
    # "12,345원" "12,345" 같은 형태에서 숫자만
    if not v or v.strip().upper() == "N/A":
        return 0
    m = re.search(r"([\d,]+)\s*원?", v)
    if not m:
        return 0
    return parse_int_kr(m.group(1))

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

        # ✅ 조달금액(추정)
        raise_krw = extract_raise_amount_krw(doc_text)

        # ✅ 종목코드(있으면 시총/현재가/할인율 계산)
        stock_code = (it.get("stock_code") or "").strip()
        if not re.fullmatch(r"\d{6}", stock_code):
            stock_code = ""

        cur_px, mcap_krw = (0, 0)
        if stock_code:
            try:
                cur_px, mcap_krw = get_price_and_mcap(stock_code)
            except Exception:
                cur_px, mcap_krw = (0, 0)

        # ✅ 시총 대비 비율(%)
        ratio_pct = None
        if raise_krw > 0 and mcap_krw > 0:
            ratio_pct = (raise_krw / mcap_krw) * 100.0

        # ✅ 할인율(%): 예정가 vs 현재가
        discount_pct = None
        issue_px = extract_issue_price_from_field(fields.get("예정가", "N/A"))
        if issue_px > 0 and cur_px > 0:
            discount_pct = (1.0 - (issue_px / cur_px)) * 100.0

        # ✅ 최대주주 참여 여부
        major_part, major_snip = extract_major_shareholder_participation(doc_text)

        msg = build_card(
            corp=corp,
            market=market_name,
            ev_type=ev_type,
            alloc=alloc,
            rcept_dt=rcept_dt,
            rpt_nm=rpt_nm,
            url=url,
            doc_text=doc_text,
            fields=fields,
            stock_code=stock_code or None,
            cur_px=cur_px,
            mcap_krw=mcap_krw,
            raise_krw=raise_krw,
            ratio_pct=ratio_pct,
            discount_pct=discount_pct,
            major_part=major_part,
            major_snip=major_snip,
        )

        tg_send_safe(msg, button_url=url)

        mark_seen(st, rno)
        sent += 1

    save_state(st)
    print(f"OK sent={sent} seen={len(st.get('seen', []))}")

if __name__ == "__main__":
    main()
