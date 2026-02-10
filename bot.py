#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, html, requests
from datetime import datetime, timedelta

DART_API_KEY = os.environ["DART_API_KEY"].strip()
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"].strip()
TG_CHAT_ID   = os.environ["TG_CHAT_ID"].strip()

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS","3"))
MIN_RAISE_KRW = int(os.getenv("MIN_RAISE_KRW","0"))
MARKET_CLASSES = [x.strip().upper() for x in os.getenv("MARKET_CLASSES","Y,K,N").split(",") if x.strip()]

INC = re.compile(r"(유상증자|유상증자결정|유무상증자|유무상증자결정)", re.I)
EXC = re.compile(r"제3자\s*배정", re.I)

LIST   = "https://opendart.fss.or.kr/api/list.json"
PIFRIC = "https://opendart.fss.or.kr/api/pifricDecsn.json"
VIEW   = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={}"
TG_SEND= "https://api.telegram.org/bot{}/sendMessage"

S = requests.Session()
S.headers.update({"User-Agent":"dart-alert-actions/1.0"})

def load_state():
    try:
        with open("state.json","r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"seen": []}

def save_state(st):
    with open("state.json","w",encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def tg_send(text, button_url=None):
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if button_url:
        payload["reply_markup"] = json.dumps({
            "inline_keyboard":[[{"text":"📄 DART 열기","url":button_url}]]
        }, ensure_ascii=False)
    r = S.post(TG_SEND.format(TG_BOT_TOKEN), data=payload, timeout=20)
    r.raise_for_status()

def market_ok(corp_cls):
    corp_cls = (corp_cls or "").strip().upper()
    return (not MARKET_CLASSES) or (corp_cls in MARKET_CLASSES)

def ok_report(name):
    if not name: return False
    if EXC.search(name): return False
    return INC.search(name) is not None

def toint(x):
    try: return int(str(x).strip().replace(",",""))
    except: return 0

def fetch_list():
    end = datetime.now().strftime("%Y%m%d")
    bgn = (datetime.now()-timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    j = S.get(LIST, params=dict(
        crtfc_key=DART_API_KEY,
        bgn_de=bgn, end_de=end,
        pblntf_ty="B",
        page_no=1, page_count=100,
        sort="date", sort_mth="desc"
    ), timeout=20).json()
    if j.get("status") == "000":
        return j.get("list", []) or []
    return []

def fetch_pifric(corp_code, rcept_dt, rcept_no):
    if not corp_code or not rcept_dt or not rcept_no:
        return None
    j = S.get(PIFRIC, params=dict(
        crtfc_key=DART_API_KEY,
        corp_code=corp_code,
        bgn_de=rcept_dt, end_de=rcept_dt
    ), timeout=20).json()
    if j.get("status") != "000":
        return None
    for it in (j.get("list",[]) or []):
        if (it.get("rcept_no") or "").strip() == rcept_no:
            return it
    return None

def funding_total(detail):
    if not detail: return 0
    return (toint(detail.get("piic_fdpp_fclt")) +
            toint(detail.get("piic_fdpp_op")) +
            toint(detail.get("piic_fdpp_dtrp")) +
            toint(detail.get("piic_fdpp_ocsa")) +
            toint(detail.get("piic_fdpp_etc")))

def main():
    st = load_state()
    seen = set(st.get("seen", []))

    items = sorted(fetch_list(), key=lambda x:(x.get("rcept_dt",""), x.get("rcept_no","")))
    new_items = []

    for it in items:
        rpt = it.get("report_nm","")
        if not ok_report(rpt): 
            continue
        if not market_ok(it.get("corp_cls")):
            continue
        rno = (it.get("rcept_no") or "").strip()
        if not rno or rno in seen:
            continue

        # 금액 필터(대표 상세에서 판단)
        det = fetch_pifric((it.get("corp_code") or "").strip(), (it.get("rcept_dt") or "").strip(), rno)
        total = funding_total(det)
        if MIN_RAISE_KRW > 0 and total < MIN_RAISE_KRW:
            continue

        new_items.append((it, det, total))

    # 전송 + seen 업데이트
    for it, det, total in new_items:
        corp = html.escape((it.get("corp_name") or "N/A").strip())
        corp_cls = (it.get("corp_cls") or "").strip().upper()
        cls_txt = {"Y":"KOSPI","K":"KOSDAQ","N":"KONEX","E":"OTHER"}.get(corp_cls, corp_cls or "N/A")

        rno = it["rcept_no"]
        url = VIEW.format(rno)

        lines = [
            f"📌 <b>증자 공시 감지</b>",
            f"• 회사: <b>{corp}</b> <i>({cls_txt})</i>",
        ]
        if det:
            mth = (det.get("piic_ic_mthn") or "").strip()
            if mth:
                lines.append(f"• 방식: <b>{html.escape(mth)}</b>")
            if total:
                lines.append(f"• 자금합계: <b>{total:,}</b>원")
        lines += ["", f"• {html.escape(it.get('rcept_dt',''))} - {html.escape(it.get('report_nm',''))}"]

        tg_send("\n".join(lines), button_url=url)
        seen.add(rno)

    # state.json에 최근 N개만 유지(너무 커지는 것 방지)
    st["seen"] = list(seen)[-5000:]
    save_state(st)

if __name__ == "__main__":
    main()
