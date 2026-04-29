"""
크린토피아 홈클리닝 Weekly Dashboard 자동 빌드 스크립트
사용법: python build_weekly.py  (크린토피아_weekly 폴더에서 실행)
필요 라이브러리: pip install pandas numpy
"""

import sys, os, json, calendar
from datetime import datetime
from urllib.parse import quote

import pandas as pd
import numpy as np

# 크린토피아_daily 폴더의 config 참조
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "크린토피아_daily"))
from config import (
    SHEET_ID, SHEET_MEDIA_RAW,
    GOOGLE_TYPE_RULES, NAVER_PC_KEYWORD,
    AUTO_BID_KEYWORDS, TARGET_CPA,
    COLOR_NAVER_BAR, COLOR_GOOGLE_BAR, COLOR_CPA_LINE,
)

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_FILE   = "template_weekly.html"
OUTPUT_FILE     = "크린토피아_Weekly_대시보드_{year}{month:02d}.html"
DAY_KO          = ["월", "화", "수", "목", "금", "토", "일"]


# ══════════════════════════════════════════
# 1. 데이터 로드
# ══════════════════════════════════════════

def load_sheet(sheet_name: str) -> pd.DataFrame:
    url = (f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
           f"/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}")
    try:
        df = pd.read_csv(url)
        print(f"  ✅ [{sheet_name}] {len(df):,}행 로드")
        return df
    except Exception as e:
        print(f"  ❌ [{sheet_name}] 실패: {e}")
        sys.exit(1)


def prepare_media_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["매체","캠페인","광고그룹","키워드","일자",
                  "노출","클릭","광고비","총전환","직접전환","간접전환","평균노출순위"]
    df["일자"] = pd.to_datetime(df["일자"])
    for col in ["노출","클릭","광고비","총전환","직접전환","간접전환","평균노출순위"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",",""), errors="coerce").fillna(0)
    df["디바이스"] = df["캠페인"].apply(lambda x: "PC" if NAVER_PC_KEYWORD in str(x) else "MO")
    def g_type(x):
        x = str(x)
        if GOOGLE_TYPE_RULES["경쟁사"] in x: return "경쟁사"
        if GOOGLE_TYPE_RULES["브랜드"] in x: return "브랜드"
        return "일반"
    df["구글유형"] = df["캠페인"].apply(g_type)
    return df


# ══════════════════════════════════════════
# 2. 주차 계산
# ══════════════════════════════════════════

def get_week_ranges(df: pd.DataFrame) -> list:
    """
    데이터의 연월을 자동 감지해 주차별 날짜 목록 반환
    주차 기준: 월의 1일부터 시작, 매주 월요일에 새 주차
    """
    active = sorted(df[df["광고비"] > 0]["일자"].dt.normalize().unique())
    if not active:
        return []

    # 가장 많은 데이터가 있는 연월 감지
    month_counts: dict = {}
    for d in active:
        key = (d.year, d.month)
        month_counts[key] = month_counts.get(key, 0) + 1
    year, month = max(month_counts, key=month_counts.get)

    _, last_day = calendar.monthrange(year, month)
    weeks, week_dates, week_num = [], [], 1

    for day in range(1, last_day + 1):
        dt = datetime(year, month, day)
        date_str = dt.strftime("%m/%d")
        if dt.weekday() == 0 and week_dates:          # 월요일 → 새 주차
            weeks.append({
                "label":  f"{month}월 {week_num}주차",
                "dates":  week_dates[:],
                "range":  f"{week_dates[0]}~{week_dates[-1]}",
                "year":   year, "month": month,
            })
            week_dates = []
            week_num  += 1
        week_dates.append(date_str)

    if week_dates:
        weeks.append({
            "label":  f"{month}월 {week_num}주차",
            "dates":  week_dates,
            "range":  f"{week_dates[0]}~{week_dates[-1]}",
            "year":   year, "month": month,
        })

    today = datetime.today().date()
    active_set = set(d.strftime("%m/%d") for d in active)

    def _week_ended(wk):
        last_day_str = wk["dates"][-1]  # "MM/DD"
        m, d = int(last_day_str.split("/")[0]), int(last_day_str.split("/")[1])
        return datetime(wk["year"], m, d).date() < today

    return [wk for wk in weeks
            if any(d in active_set for d in wk["dates"]) and _week_ended(wk)]


# ══════════════════════════════════════════
# 3. KPI 집계 헬퍼
# ══════════════════════════════════════════

def _agg(df: pd.DataFrame, dates: list, has_rank: bool = True) -> dict:
    sub = df[df["일자"].dt.strftime("%m/%d").isin(dates)]
    if len(sub) == 0:
        return {"imp":0,"clk":0,"spend":0,"conv":0.0,"ctr":0.0,"cpc":0,"cvr":0.0,"cpa":0,"rank":0.0}
    imp = int(sub["노출"].sum()); clk = int(sub["클릭"].sum())
    sp  = int(sub["광고비"].sum()); cv = float(round(float(sub["총전환"].sum()), 1))
    rk  = float(round(float(sub["평균노출순위"].mean()), 2)) if has_rank else 0.0
    return {
        "imp": imp, "clk": clk, "spend": sp, "conv": cv,
        "ctr":  float(round(clk/imp*100, 3)) if imp > 0 else 0.0,
        "cpc":  int(round(sp/clk))           if clk > 0 else 0,
        "cvr":  float(round(cv/clk*100, 2))  if clk > 0 else 0.0,
        "cpa":  int(round(sp/cv))            if cv  > 0 else 0,
        "rank": rk,
    }


def _agg_kw(df: pd.DataFrame, dates: list) -> list:
    sub = df[df["일자"].dt.strftime("%m/%d").isin(dates)]
    result = []
    for kw, grp in sub.groupby("키워드"):
        sp = int(grp["광고비"].sum()); cv = float(round(float(grp["총전환"].sum()),1))
        cl = int(grp["클릭"].sum());   im = int(grp["노출"].sum())
        result.append({
            "kw": str(kw), "spend": sp, "conv": cv, "clk": cl, "imp": im,
            "cpc": int(round(sp/cl)) if cl>0 else 0,
            "cvr": float(round(cv/cl*100,2)) if cl>0 else 0.0,
            "cpa": int(round(sp/cv)) if cv>0 else 0,
            "ctr": float(round(cl/im*100,3)) if im>0 else 0.0,
        })
    result.sort(key=lambda x: -x["conv"])   # 전환 내림차순
    return result


# ══════════════════════════════════════════
# 4. 주차별 전체 KPI JSON
# ══════════════════════════════════════════

def build_all_weekly_kpi_json(
    naver, naver_pc, naver_mo,
    google, google_b, google_c, google_g,
    naver_df, weeks
) -> str:
    result = {}
    for wk in weeks:
        dates, year = wk["dates"], wk["year"]

        npc = _agg(naver_pc, dates)
        nmo = _agg(naver_mo, dates)
        gb  = _agg(google_b, dates, has_rank=False)
        gc  = _agg(google_c, dates, has_rank=False)
        gg  = _agg(google_g, dates, has_rank=False)

        # 네이버 통합 (PC+MO)
        n_clk = npc["clk"]+nmo["clk"]; n_sp = npc["spend"]+nmo["spend"]
        n_cv  = round(npc["conv"]+nmo["conv"], 1)

        # 일별 breakdown
        daily_rows: dict = {}
        for key, df_ in [("npc",naver_pc),("nmo",naver_mo),
                          ("gb",google_b),("gc",google_c),("gg",google_g)]:
            has_r = key in ("npc","nmo")
            rows  = []
            for d in dates:
                r = _agg(df_, [d], has_rank=has_r)
                mm, dd = int(d.split("/")[0]), int(d.split("/")[1])
                r["date"] = d
                r["day"]  = DAY_KO[datetime(year, mm, dd).weekday()]
                rows.append(r)
            daily_rows[key] = rows

        # 자동입찰 주간 합산 + 일별
        auto_weekly: dict = {}
        auto_daily:  dict = {}
        for kw in AUTO_BID_KEYWORDS:
            auto_weekly[kw] = {}
            auto_daily[kw]  = {}
            for dev in ["MO","PC"]:
                sub = naver_df[
                    (naver_df["디바이스"] == dev) &
                    (naver_df["키워드"]  == kw)  &
                    (naver_df["일자"].dt.strftime("%m/%d").isin(dates))
                ]
                sp = int(sub["광고비"].sum()); cv = float(sub["총전환"].sum())
                cl = int(sub["클릭"].sum());   im = int(sub["노출"].sum())
                rk = float(round(float(sub["평균노출순위"].mean()),1)) if len(sub)>0 else 0.0
                auto_weekly[kw][dev] = {
                    "spend": sp, "conv": round(cv,1), "imp": im,
                    "cpa": int(round(sp/cv)) if cv>0 else 0,
                    "cpc": int(round(sp/cl)) if cl>0 else 0,
                    "rank": rk,
                }
                day_rows = []
                for d in dates:
                    ds = naver_df[
                        (naver_df["디바이스"] == dev) &
                        (naver_df["키워드"]  == kw)  &
                        (naver_df["일자"].dt.strftime("%m/%d") == d)
                    ]
                    dsp=int(ds["광고비"].sum()); dcv=float(ds["총전환"].sum())
                    dcl=int(ds["클릭"].sum());   dim=int(ds["노출"].sum())
                    drk=float(round(float(ds["평균노출순위"].mean()),1)) if len(ds)>0 else 0.0
                    mm2,dd2=int(d.split("/")[0]),int(d.split("/")[1])
                    day_rows.append({
                        "date":d,"day":DAY_KO[datetime(year,mm2,dd2).weekday()],
                        "spend":dsp,"conv":round(dcv,1),"imp":dim,
                        "cpa":int(round(dsp/dcv)) if dcv>0 else 0,
                        "cpc":int(round(dsp/dcl)) if dcl>0 else 0,
                        "rank":drk,
                    })
                auto_daily[kw][dev] = day_rows

        result[wk["label"]] = {
            "dates": dates, "range": wk["range"],
            "n_spend": n_sp, "n_conv": n_cv,
            "n_cpa":  int(round(n_sp/n_cv))  if n_cv>0  else 0,
            "n_cpc":  int(round(n_sp/n_clk)) if n_clk>0 else 0,
            "n_cvr":  float(round(n_cv/n_clk*100,2)) if n_clk>0 else 0.0,
            "g_spend": gb["spend"]+gc["spend"]+gg["spend"],
            "g_conv":  round(gb["conv"]+gc["conv"]+gg["conv"],1),
            "g_cpa":   (lambda gs,gc_: int(round(gs/gc_)) if gc_>0 else 0)(
                           gb["spend"]+gc["spend"]+gg["spend"],
                           gb["conv"]+gc["conv"]+gg["conv"]),
            "g_cpc":   (lambda gs,gk: int(round(gs/gk)) if gk>0 else 0)(
                           gb["spend"]+gc["spend"]+gg["spend"],
                           gb["clk"]+gc["clk"]+gg["clk"]),
            "g_cvr":   (lambda gc_,gk: float(round(gc_/gk*100,2)) if gk>0 else 0.0)(
                           gb["conv"]+gc["conv"]+gg["conv"],
                           gb["clk"]+gc["clk"]+gg["clk"]),
            "npc": npc, "nmo": nmo, "gb": gb, "gc": gc, "gg": gg,
            "npc_kw": _agg_kw(naver_pc, dates),
            "nmo_kw": _agg_kw(naver_mo, dates),
            "gb_kw":  _agg_kw(google_b, dates),
            "gc_kw":  _agg_kw(google_c, dates),
            "gg_kw":  _agg_kw(google_g, dates),
            "daily_rows":  daily_rows,
            "auto_weekly": auto_weekly,
            "auto_daily":  auto_daily,
        }
    return json.dumps(result, ensure_ascii=False)


# ══════════════════════════════════════════
# 5. 주간 코멘트 생성
# ══════════════════════════════════════════

def _fmt(v: float) -> str:
    return f"{v/10000:.1f}만원" if v >= 10000 else f"{v:,}원"

def _pct(curr, prev):
    return round((curr-prev)/prev*100) if prev and prev != 0 else None

_LOCAL_WORDS = [
    "서울","부산","대구","인천","광주","대전","울산","세종",
    "강남","강서","강북","강동","마포","송파","관악","서초","노원","은평","동작","성북","종로","중구","용산",
    "수원","성남","고양","용인","창원","청주","전주","천안","안산","남양주","화성","안양","부천",
    "평택","의정부","시흥","하남","광명","군포","오산","이천","경기","강원",
    "충북","충남","전북","전남","경북","경남","제주",
]
def _kw_type(kw: str) -> str:
    if "크린토피아" in kw: return "브랜드"
    if any(loc in kw for loc in _LOCAL_WORDS): return "지역"
    return "메인"


def generate_weekly_comment(wk: dict, prev: dict | None) -> dict:
    hp = prev is not None

    def sbadge(c, p):
        if not hp: return ""
        pp = _pct(c, p)
        return f"전주({_fmt(p)}) 대비 {'+' if pp and pp>=0 else ''}{pp}%" if pp is not None else ""

    def cbadge(c, p):
        if not hp or not p: return ""
        return f"전주({p:,}원) 대비 {'개선' if c<p else '상승'}"

    sections = {}

    # ── Summary ──
    npc=wk["npc"]; nmo=wk["nmo"]
    n_sp=npc["spend"]+nmo["spend"]; n_cv=round(npc["conv"]+nmo["conv"],1)
    n_cpa=int(round(n_sp/n_cv)) if n_cv>0 else 0
    gb=wk["gb"]; gc=wk["gc"]; gg=wk["gg"]
    g_sp=gb["spend"]+gc["spend"]+gg["spend"]; g_cv=round(gb["conv"]+gc["conv"]+gg["conv"],1)
    g_cpa=int(round(g_sp/g_cv)) if g_cv>0 else 0
    lines=[]
    if hp:
        pnpc=prev["npc"]; pnmo=prev["nmo"]
        pn_sp=pnpc["spend"]+pnmo["spend"]; pn_cv=round(pnpc["conv"]+pnmo["conv"],1)
        pn_cpa=int(round(pn_sp/pn_cv)) if pn_cv>0 else 0
        pgb=prev["gb"]; pgc=prev["gc"]; pgg=prev["gg"]
        pg_sp=pgb["spend"]+pgc["spend"]+pgg["spend"]; pg_cv=round(pgb["conv"]+pgc["conv"]+pgg["conv"],1)
        pg_cpa=int(round(pg_sp/pg_cv)) if pg_cv>0 else 0
        np_=_pct(n_sp,pn_sp); gp_=_pct(g_sp,pg_sp)
        lines.append(
            f"네이버 SA 주간 광고비 {_fmt(n_sp)}, 전환 {n_cv}건, CPA {n_cpa:,}원 — "
            f"전주({_fmt(pn_sp)}, 전환 {pn_cv}건, CPA {pn_cpa:,}원) 대비 "
            f"광고비 {'+' if np_ and np_>=0 else ''}{np_}%, CPA {'개선' if n_cpa<pn_cpa else '상승'}."
        )
        lines.append(
            f"구글 SA 주간 광고비 {_fmt(g_sp)}, 전환 {g_cv}건, CPA {g_cpa:,}원 — "
            f"전주({_fmt(pg_sp)}, 전환 {pg_cv}건, CPA {pg_cpa:,}원) 대비 "
            f"광고비 {'+' if gp_ and gp_>=0 else ''}{gp_}%, CPA {'개선' if g_cpa<pg_cpa else '상승'}."
        )
    else:
        lines.append(f"네이버 SA 주간 광고비 {_fmt(n_sp)}, 전환 {n_cv}건, CPA {n_cpa:,}원.")
        lines.append(f"구글 SA 주간 광고비 {_fmt(g_sp)}, 전환 {g_cv}건, CPA {g_cpa:,}원.")
    sections["SUMMARY"] = "\n".join(lines)

    # ── 채널별 ──
    def ch(curr, pv, kws):
        lines = []
        # 줄1: KPI 요약
        goal_s = f" (목표{'이내' if curr['cpa']<=TARGET_CPA else '초과'})" if curr["cpa"] > 0 else ""
        cpa_s = f"{curr['cpa']:,}원{goal_s}" if curr["conv"] > 0 else "전환 미발생"
        base = f"광고비 {_fmt(curr['spend'])}, 전환 {curr['conv']}건, CPA {cpa_s}"
        if pv:
            sb = sbadge(curr["spend"], pv["spend"])
            cb = cbadge(curr["cpa"], pv["cpa"]) if curr["conv"] > 0 else ""
            suffix = " — " + "; ".join(x for x in [sb, cb] if x) if (sb or cb) else ""
            lines.append(base + suffix + ".")
        else:
            lines.append(base + ".")
        # 줄2: 전환 상위 키워드 (특성별 3~4개)
        conv_kws = sorted([k for k in kws if k["conv"] > 0], key=lambda x: -x["conv"])
        if conv_kws:
            by_type = {"메인": [], "지역": [], "브랜드": []}
            for k in conv_kws:
                by_type[_kw_type(k["kw"])].append(k)
            selected = []
            for t in ["메인", "지역", "브랜드"]:
                limit = 2 if t == "메인" else 1
                selected.extend(by_type[t][:limit])
            selected = sorted(selected, key=lambda x: -x["conv"])[:4]
            kw_lines = [f"  {_kw_type(k['kw'])}) [{k['kw']}] 전환 {k['conv']}건 / CPA {k['cpa']:,}원"
                        for k in selected]
            lines.append("전환 상위 키워드||" + "||".join(kw_lines))
        else:
            lines.append("전환 상위 키워드: 해당 없음 (전환 미발생).")
        # 줄3: 광고비 상위 키워드 (5개, 줄바꿈)
        top5 = sorted(kws, key=lambda x: -x["spend"])[:5]
        if top5:
            kw_lines = []
            for i, k in enumerate(top5, 1):
                kt = _kw_type(k["kw"])
                type_tag = f"({kt})" if kt != "메인" else ""
                perf = f"전환 {k['conv']}건 / CPA {k['cpa']:,}원" if k["conv"] > 0 else "전환 미발생"
                kw_lines.append(f"  {i}. [{k['kw']}]{type_tag} {_fmt(k['spend'])} / {perf}")
            lines.append("광고비 상위 키워드||" + "||".join(kw_lines))
        return "\n".join(lines)

    sections["N_PC"]    = ch(wk["npc"], prev["npc"] if hp else None, wk.get("npc_kw", []))
    sections["N_MO"]    = ch(wk["nmo"], prev["nmo"] if hp else None, wk.get("nmo_kw", []))
    sections["G_BRAND"] = ch(wk["gb"],  prev["gb"]  if hp else None, wk.get("gb_kw",  []))
    sections["G_COMP"]  = ch(wk["gc"],  prev["gc"]  if hp else None, wk.get("gc_kw",  []))
    sections["G_GEN"]   = ch(wk["gg"],  prev["gg"]  if hp else None, wk.get("gg_kw",  []))

    # ── 자동입찰 ──
    al = []
    for dev in ["MO","PC"]:
        for kw in AUTO_BID_KEYWORDS:
            ca = wk["auto_weekly"].get(kw,{}).get(dev,{})
            pa = prev["auto_weekly"].get(kw,{}).get(dev,{}) if hp else {}
            tag = f"[{dev}] {kw}"
            if not ca or ca.get("spend",0)==0:
                al.append(f"<strong>{tag}</strong> — 주간 데이터 없음."); continue
            line = f"<strong>{tag}</strong> — 주간 전환 {ca['conv']}건"
            if ca.get("cpa",0)>0: line += f" CPA {ca['cpa']:,}원"
            if ca.get("rank",0)>0: line += f". 노출순위 평균 {ca['rank']}위"
            if pa and pa.get("spend",0)>0:
                cv_dir = "전환 증가" if ca["conv"]>pa["conv"] else ("전환 감소" if ca["conv"]<pa["conv"] else "전환 동일")
                cpa_pfx=""
                if ca.get("cpa",0)>0 and pa.get("cpa",0)>0:
                    cpa_pfx=("CPA 개선" if ca["cpa"]<pa["cpa"] else "CPA 상승")+" · "
                line += f" — 전주 대비 {cpa_pfx}{cv_dir}."
            else:
                line += "."
            al.append(line)
    sections["AUTO"] = "\n".join(al)

    return sections


def build_all_weekly_comments_json(all_weekly: dict, weeks: list) -> str:
    labels = [wk["label"] for wk in weeks]
    result = {}
    for i, wk in enumerate(weeks):
        lbl  = wk["label"]
        curr = all_weekly[lbl]
        prev = all_weekly[labels[i-1]] if i > 0 else None
        result[lbl] = generate_weekly_comment(curr, prev)
    return json.dumps(result, ensure_ascii=False)


# ══════════════════════════════════════════
# 6. 메인
# ══════════════════════════════════════════

def main():
    print("="*55)
    print("  크린토피아 Weekly Dashboard 자동 빌드")
    print("="*55)

    print("\n[1] 구글 시트 로드 중...")
    raw_df = load_sheet(SHEET_MEDIA_RAW)
    df     = prepare_media_raw(raw_df)

    print("\n[2] 주차 계산 중...")
    weeks = get_week_ranges(df)
    active_set = set(df[df["광고비"]>0]["일자"].dt.strftime("%m/%d"))
    for wk in weeks:
        nd = len([d for d in wk["dates"] if d in active_set])
        print(f"  {wk['label']}: {wk['range']} ({nd}일 데이터)")

    naver   = df[df["매체"]=="네이버 SA"]
    google  = df[df["매체"]=="구글 SA"]
    naver_pc= naver[naver["디바이스"]=="PC"]
    naver_mo= naver[naver["디바이스"]=="MO"]
    google_b= google[google["구글유형"]=="브랜드"]
    google_c= google[google["구글유형"]=="경쟁사"]
    google_g= google[google["구글유형"]=="일반"]

    print("\n[3] 주차별 KPI 집계 중...")
    all_kpi_json = build_all_weekly_kpi_json(
        naver, naver_pc, naver_mo,
        google, google_b, google_c, google_g,
        naver, weeks
    )
    all_kpi = json.loads(all_kpi_json)

    print("\n[4] 주간 코멘트 생성 중...")
    all_cmt_json = build_all_weekly_comments_json(all_kpi, weeks)

    latest       = weeks[-1]
    year, month  = latest["year"], latest["month"]
    weeks_lbl_js = json.dumps([wk["label"] for wk in weeks], ensure_ascii=False)
    update_date  = datetime.now().strftime("%Y/%m/%d %H:%M")

    print("\n[5] 템플릿 렌더링 중...")
    tpl_path = os.path.join(BASE_DIR, TEMPLATE_FILE)
    with open(tpl_path, encoding="utf-8") as f:
        html = f.read()

    for k, v in {
        "{{WEEKS_LIST_JSON}}":          weeks_lbl_js,
        "{{ALL_WEEKLY_KPI_JSON}}":      all_kpi_json,
        "{{ALL_WEEKLY_COMMENTS_JSON}}": all_cmt_json,
        "{{TARGET_CPA}}":               str(TARGET_CPA),
        "{{UPDATE_DATE}}":              update_date,
        "{{YEAR}}":                     str(year),
        "{{MONTH}}":                    str(month),
        "{{COLOR_NAVER_BAR}}":          COLOR_NAVER_BAR,
        "{{COLOR_GOOGLE_BAR}}":         COLOR_GOOGLE_BAR,
        "{{COLOR_CPA_LINE}}":           COLOR_CPA_LINE,
    }.items():
        html = html.replace(k, str(v))

    out_name = OUTPUT_FILE.format(year=year, month=month)
    out_path = os.path.join(BASE_DIR, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    with open(os.path.join(BASE_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n{'='*55}")
    print(f"  ✅ 완료! → {out_name}")
    print(f"{'='*55}")
    print("  브라우저에서 파일을 열어 확인하세요.")


if __name__ == "__main__":
    main()
