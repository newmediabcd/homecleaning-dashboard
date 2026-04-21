"""
크린토피아 홈클리닝 Daily Report 자동 빌드 스크립트
사용법: python build_dashboard.py

필요 라이브러리: pip install pandas numpy anthropic
"""

import pandas as pd
import numpy as np
import json
import re
import sys
import os
from urllib.parse import quote
from datetime import datetime

# ──────────────────────────────────────────
# 설정 로드
# ──────────────────────────────────────────
from config import (
    SHEET_ID, SHEET_MEDIA_RAW, SHEET_SUMMARY, SHEET_GA4,
    OUTPUT_FILENAME, TEMPLATE_FILENAME,
    GOOGLE_TYPE_RULES, NAVER_PC_KEYWORD,
    AUTO_BID_KEYWORDS, TARGET_CPA,
    COLOR_NAVER_BAR, COLOR_GOOGLE_BAR,
    COLOR_CPA_LINE, COLOR_CAC_LINE, COLOR_GA_LINE,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ══════════════════════════════════════════
# 1. 데이터 로드
# ══════════════════════════════════════════

def load_sheet(sheet_name: str) -> pd.DataFrame:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
        f"/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}"
    )
    try:
        df = pd.read_csv(url)
        print(f"  ✅ [{sheet_name}] {len(df):,}행 로드")
        return df
    except Exception as e:
        print(f"  ❌ [{sheet_name}] 실패: {e}")
        sys.exit(1)


def prepare_media_raw(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        "매체", "캠페인", "광고그룹", "키워드", "일자",
        "노출", "클릭", "광고비", "총전환", "직접전환", "간접전환", "평균노출순위"
    ]
    df["일자"] = pd.to_datetime(df["일자"])
    for col in ["노출", "클릭", "광고비", "총전환", "직접전환", "간접전환", "평균노출순위"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0)

    # 디바이스 / 캠페인 유형 분류
    df["디바이스"] = df["캠페인"].apply(
        lambda x: "PC" if NAVER_PC_KEYWORD in str(x) else "MO"
    )
    def g_type(x):
        x = str(x)
        if GOOGLE_TYPE_RULES["경쟁사"] in x: return "경쟁사"
        if GOOGLE_TYPE_RULES["브랜드"] in x: return "브랜드"
        return "일반"
    df["구글유형"] = df["캠페인"].apply(g_type)
    return df


# ══════════════════════════════════════════
# 2. 날짜 메타 계산
# ══════════════════════════════════════════

def get_date_meta(df: pd.DataFrame) -> dict:
    active = df[df["광고비"] > 0]["일자"].unique()
    active = sorted(active)

    curr = max(active)
    prev = active[-2] if len(active) >= 2 else curr

    all_dates = [d.strftime("%m/%d") for d in active]

    day_ko = ["월", "화", "수", "목", "금", "토", "일"]

    return {
        "curr":        curr,
        "prev":        prev,
        "curr_str":    curr.strftime("%m/%d"),
        "prev_str":    prev.strftime("%m/%d"),
        "curr_day":    day_ko[curr.weekday()],
        "prev_day":    day_ko[prev.weekday()],
        "curr_label":  curr.strftime("%Y/%m/%d"),
        "data_start":  active[0].strftime("%m/%d"),
        "data_end":    curr.strftime("%m/%d"),
        "all_dates":   all_dates,
        "recent7":     all_dates[-7:],
        "update_str":  datetime.now().strftime("%Y.%m.%d") + f"({day_ko[datetime.now().weekday()]}) 업데이트",
    }


# ══════════════════════════════════════════
# 3. KPI 집계 헬퍼
# ══════════════════════════════════════════

def daily_kpi(df: pd.DataFrame, date_str: str) -> dict:
    d = df[df["일자"].dt.strftime("%m/%d") == date_str]
    spend = d["광고비"].sum()
    conv  = d["총전환"].sum()
    clk   = d["클릭"].sum()
    return {
        "spend": spend,
        "conv":  conv,
        "cpa":   round(spend / conv, 0) if conv > 0 else 0,
        "cvr":   round(conv / clk * 100, 2) if clk > 0 else 0,
    }


def fmt_spend(val: float) -> str:
    return f"{val/10000:.1f}만원"


def diff_badge(curr_val, prev_val, reverse=False) -> tuple:
    """(표시문자열, CSS클래스)  reverse=True → 수치가 낮을수록 좋음(CPA 등)"""
    if prev_val == 0:
        return "전일 대비 —", "kn"
    pct = round((curr_val - prev_val) / prev_val * 100)
    sign = "+" if pct >= 0 else ""
    if reverse:
        cls = "ku" if curr_val < prev_val else "kd"
        label = "개선" if curr_val < prev_val else "상승"
        return f"전일 {fmt_spend(prev_val) if prev_val>9999 else f'{int(prev_val):,}원'} 대비 {label}", cls
    else:
        cls = "ku" if pct < 0 else "kd"
        return f"전일 {fmt_spend(prev_val)} 대비 {sign}{pct}%", cls


# ══════════════════════════════════════════
# 4. 차트 데이터 생성
# ══════════════════════════════════════════

def js_arr(values) -> str:
    """Python 리스트 → JS 배열 문자열"""
    def fmt(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "null"
        if isinstance(v, float):
            return str(round(v, 2))
        return str(int(v))
    return "[" + ",".join(fmt(v) for v in values) + "]"


def make_bar_color(n_total: int, color_template: str) -> str:
    """n_total개 중 마지막만 진하게"""
    light = color_template.format(a="0.55")
    dark  = color_template.format(a="0.9")
    arr = [f'"{light}"'] * (n_total - 1) + [f'"{dark}"']
    return "[" + ",".join(arr) + "]"


def daily_series(df: pd.DataFrame, dates: list, col: str) -> list:
    """날짜 리스트 순서대로 col 합산값 반환 (없으면 0 또는 null)"""
    result = []
    for d in dates:
        sub = df[df["일자"].dt.strftime("%m/%d") == d]
        val = sub[col].sum() if len(sub) > 0 else 0
        result.append(val)
    return result


def daily_cpa_series(df: pd.DataFrame, dates: list) -> list:
    result = []
    for d in dates:
        sub = df[df["일자"].dt.strftime("%m/%d") == d]
        spend = sub["광고비"].sum()
        conv  = sub["총전환"].sum()
        result.append(round(spend/conv, 0) if conv > 0 else None)
    return result


def build_all_daily_kpi_json(naver_df, naver_pc_df, naver_mo_df, google_df, google_b_df, google_c_df, google_g_df, all_dates):
    """전체 날짜별 KPI + 기기별 표 데이터 JSON 생성 (날짜 선택 필터용)"""
    def _row(sub, has_rank=True):
        if len(sub) == 0:
            return {"imp":0,"clk":0,"spend":0,"conv":0.0,"ctr":0.0,"cpc":0,"cvr":0.0,"cpa":0,"rank":0.0}
        imp   = int(sub["노출"].sum())
        clk   = int(sub["클릭"].sum())
        spend = int(sub["광고비"].sum())
        conv  = float(round(float(sub["총전환"].sum()), 1))
        rank  = float(round(float(sub["평균노출순위"].mean()), 2)) if has_rank else 0.0
        ctr   = float(round(clk/imp*100, 3)) if imp > 0 else 0.0
        cpc   = int(round(spend/clk, 0)) if clk > 0 else 0
        cvr   = float(round(conv/clk*100, 2)) if clk > 0 else 0.0
        cpa   = int(round(spend/conv, 0)) if conv > 0 else 0
        return {"imp":imp,"clk":clk,"spend":spend,"conv":conv,"ctr":ctr,"cpc":cpc,"cvr":cvr,"cpa":cpa,"rank":rank}

    result = {}
    for d in all_dates:
        n = daily_kpi(naver_df, d)
        g = daily_kpi(google_df, d)
        npc_sub = naver_pc_df[naver_pc_df["일자"].dt.strftime("%m/%d") == d]
        nmo_sub = naver_mo_df[naver_mo_df["일자"].dt.strftime("%m/%d") == d]
        g_sub   = google_df[google_df["일자"].dt.strftime("%m/%d") == d]
        gb_sub  = google_b_df[google_b_df["일자"].dt.strftime("%m/%d") == d]
        gc_sub  = google_c_df[google_c_df["일자"].dt.strftime("%m/%d") == d]
        gg_sub  = google_g_df[google_g_df["일자"].dt.strftime("%m/%d") == d]
        result[d] = {
            "n_spend": int(n["spend"]),
            "n_conv":  float(round(float(n["conv"]), 1)),
            "n_cpa":   int(n["cpa"]),
            "g_spend": int(g["spend"]),
            "g_conv":  float(round(float(g["conv"]), 1)),
            "g_cpa":   int(g["cpa"]),
            "npc": _row(npc_sub, True),
            "nmo": _row(nmo_sub, True),
            "g":   _row(g_sub,   False),
            "gb":  _row(gb_sub,  False),
            "gc":  _row(gc_sub,  False),
            "gg":  _row(gg_sub,  False),
        }
    return json.dumps(result, ensure_ascii=False)


def parse_summary_sheet(df: pd.DataFrame) -> dict:
    """전체_summary 시트 → 매체별 날짜 키 딕셔너리 반환
    반환: {"naver": {MM/DD: {recv, cac, ga_recv, ga_rate}}, "google": {...}}
    col14=내부접수완료, col15=CAC, col16=견적예약버튼클릭, col17=GA예약완료
    """
    import re

    def parse_date(s):
        m = re.match(r'\d{4}\.\s*(\d+)\.\s*(\d+)', str(s))
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}" if m else None

    def to_num(v):
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

    result = {}
    # 네이버 daily: 행 6~35 / 구글 daily: 행 39~68 (첫 행이 헤더로 소비되어 -1 offset)
    for media, rows in [("naver", df.iloc[6:36]), ("google", df.iloc[39:69])]:
        data = {}
        for i in range(len(rows)):
            row = rows.iloc[i]
            d = parse_date(row.iloc[1])
            if d is None:
                continue
            spend = to_num(row.iloc[4])
            if not spend or spend == 0:
                continue          # 미래 날짜 / 데이터 없는 날 제외
            recv      = to_num(row.iloc[14]) or 0
            cac       = to_num(row.iloc[15])
            ga_click  = to_num(row.iloc[16]) or 0
            ga_recv   = to_num(row.iloc[17]) or 0
            ga_rate   = round(ga_recv / ga_click * 100, 1) if ga_click > 0 else None
            data[d] = {
                "recv":    recv if recv > 0 else None,
                "cac":     cac  if (cac and cac > 0) else None,
                "ga_recv": ga_recv if ga_recv > 0 else None,
                "ga_rate": ga_rate,
            }
        result[media] = data
    return result


def build_chart_data(naver_df, google_df, all_dates, summary_data=None):
    n = len(all_dates)

    # 네이버 전환 + CPA (PL 기준)
    n_conv  = daily_series(naver_df, all_dates, "총전환")
    n_cpa   = daily_cpa_series(naver_df, all_dates)

    # 구글 전환 + CPA (PL 기준)
    g_conv  = daily_series(google_df, all_dates, "총전환")
    g_cpa   = daily_cpa_series(google_df, all_dates)

    # 내부 접수완료 / CAC / GA 예약완료 / 완료율
    if summary_data:
        def _get(media, d, key):
            return summary_data[media].get(d, {}).get(key, None)
        n_recv    = [_get("naver",  d, "recv")    for d in all_dates]
        n_cac     = [_get("naver",  d, "cac")     for d in all_dates]
        n_ga_recv = [_get("naver",  d, "ga_recv") for d in all_dates]
        n_ga_rate = [_get("naver",  d, "ga_rate") for d in all_dates]
        g_recv    = [_get("google", d, "recv")    for d in all_dates]
        g_cac     = [_get("google", d, "cac")     for d in all_dates]
        g_ga_recv = [_get("google", d, "ga_recv") for d in all_dates]
        g_ga_rate = [_get("google", d, "ga_rate") for d in all_dates]
    else:
        null_arr  = [None] * n
        n_recv = n_cac = n_ga_recv = n_ga_rate = null_arr
        g_recv = g_cac = g_ga_recv = g_ga_rate = null_arr

    return {
        "BAR_CN_CV":   js_arr(n_conv),
        "LINE_CN_CV":  js_arr(n_cpa),
        "BAR_CN_RECV": js_arr(n_recv),
        "LINE_CN_RECV":js_arr(n_cac),
        "BAR_CN_GA":   js_arr(n_ga_recv),
        "LINE_CN_GA":  js_arr(n_ga_rate),
        "BAR_CG_CV":   js_arr(g_conv),
        "LINE_CG_CV":  js_arr(g_cpa),
        "BAR_CG_RECV": js_arr(g_recv),
        "LINE_CG_RECV":js_arr(g_cac),
        "BAR_CG_GA":   js_arr(g_ga_recv),
        "LINE_CG_GA":  js_arr(g_ga_rate),
    }


# ══════════════════════════════════════════
# 5. 자동입찰 autoDetail JS 생성
# ══════════════════════════════════════════

def build_auto_detail_js(naver_df: pd.DataFrame, all_dates: list) -> str:
    """autoDetail const JS 블록 생성"""

    def kw_data(dev, kw):
        rows = {}
        for d in all_dates:
            sub = naver_df[
                (naver_df["디바이스"] == dev) &
                (naver_df["키워드"] == kw) &
                (naver_df["일자"].dt.strftime("%m/%d") == d)
            ]
            if len(sub) == 0:
                rows[d] = dict(imp=0,clk=0,spend=0,ctr=0,cpc=0,conv=0,cvr=0,cpa=0,rank=0)
            else:
                imp   = int(sub["노출"].sum())
                clk   = int(sub["클릭"].sum())
                spend = sub["광고비"].sum()
                conv  = sub["총전환"].sum()
                rank  = sub["평균노출순위"].mean()
                rows[d] = dict(
                    imp=imp, clk=clk, spend=int(spend),
                    ctr=round(clk/imp*100,2) if imp>0 else 0,
                    cpc=int(round(spend/clk,0)) if clk>0 else 0,
                    conv=round(conv,1),
                    cvr=round(conv/clk*100,1) if clk>0 else 0,
                    cpa=int(round(spend/conv,0)) if conv>0 else 0,
                    rank=round(rank,1) if rank>0 else 0,
                )
        return rows

    def arr(rows, key):
        vals = [rows[d][key] for d in all_dates]
        return js_arr([v if v != 0 else (None if key=="cpa" else 0) for v in vals])

    lines = ["// 자동입찰 상세 데이터 (노출, 클릭, 광고비, CTR, CPC, 전환, CVR, CPA, 순위)"]
    lines.append("const autoDetail = {")

    for dev in ["PC", "MO"]:
        lines.append(f"  {dev}: {{")
        for kw in AUTO_BID_KEYWORDS:
            rows = kw_data(dev, kw)
            lines.append(f'    "{kw}": {{')
            lines.append(f'      imp:{arr(rows,"imp")},')
            lines.append(f'      clk:{arr(rows,"clk")},')
            lines.append(f'      spend:{arr(rows,"spend")},')
            lines.append(f'      ctr:{arr(rows,"ctr")},')
            lines.append(f'      cpc:{arr(rows,"cpc")},')
            lines.append(f'      conv:{arr(rows,"conv")},')
            lines.append(f'      cvr:{arr(rows,"cvr")},')
            lines.append(f'      cpa:{arr(rows,"cpa")},')
            lines.append(f'      rank:{arr(rows,"rank")}')
            lines.append("    },")
        lines.append("  },")

    lines.append("};")
    return "\n".join(lines)


# ══════════════════════════════════════════
# 6. 코멘트 자동 생성 (규칙 기반)
# ══════════════════════════════════════════

def _kpi(df, d):
    sub = df[df["일자"].dt.strftime("%m/%d") == d]
    sp = sub["광고비"].sum(); cv = sub["총전환"].sum(); cl = sub["클릭"].sum()
    return {
        "spend": int(sp), "conv": round(float(cv), 1),
        "cpa": int(round(sp / cv)) if cv > 0 else 0,
        "cvr": round(float(cv) / cl * 100, 2) if cl > 0 else 0,
    }


def _kw_day(df, d, top_n=12):
    sub = df[df["일자"].dt.strftime("%m/%d") == d]
    if len(sub) == 0:
        return []
    grp = sub.groupby("키워드").agg({"광고비": "sum", "총전환": "sum", "클릭": "sum"}).reset_index()
    grp = grp[grp["광고비"] > 0].sort_values("광고비", ascending=False).head(top_n)
    out = []
    for _, r in grp.iterrows():
        sp, cv, cl = float(r["광고비"]), float(r["총전환"]), int(r["클릭"])
        out.append({
            "kw": r["키워드"], "spend": int(sp), "conv": round(cv, 1),
            "cpa": int(round(sp / cv)) if cv > 0 else 0,
            "cvr": round(cv / cl * 100, 2) if cl > 0 else 0,
        })
    return out


def _kw_wd_avg(df, recent_wd):
    """키워드별 평일 일평균 광고비"""
    if not recent_wd:
        return {}
    sub = df[df["일자"].dt.strftime("%m/%d").isin(recent_wd)]
    grp = sub.groupby("키워드")["광고비"].sum().reset_index()
    return {r["키워드"]: int(r["광고비"] / len(recent_wd)) for _, r in grp.iterrows() if r["광고비"] > 0}


def build_comment_data(naver_pc, naver_mo, google_brand, google_comp, google_gen,
                       naver_df, meta, all_dates):
    curr, prev = meta["curr_str"], meta["prev_str"]

    # 최근 평일(월~금) 5일 (당일 제외)
    recent_wd = [
        d for d in all_dates
        if d != curr and pd.Timestamp(str(meta["curr"].year) + "-" + d.replace("/", "-")).weekday() < 5
    ][-5:]

    def wd_avg(df):
        vals = [_kpi(df, d) for d in recent_wd]
        if not vals:
            return {"spend": 0, "conv": 0, "cpa": 0, "dates": ""}
        sp = int(sum(v["spend"] for v in vals) / len(vals))
        cv = round(sum(v["conv"] for v in vals) / len(vals), 1)
        cpa_vals = [v["cpa"] for v in vals if v["cpa"] > 0]
        return {
            "spend": sp, "conv": cv,
            "cpa": int(sum(cpa_vals) / len(cpa_vals)) if cpa_vals else 0,
            "dates": f"{recent_wd[0]}~{recent_wd[-1]}" if recent_wd else "",
        }

    # 자동입찰 최근 5일 키워드별 데이터
    recent5 = all_dates[-5:]

    def auto_kw_rows(dev, kw):
        first_spend = None
        for d in all_dates:
            sub = naver_df[
                (naver_df["디바이스"] == dev) & (naver_df["키워드"] == kw) &
                (naver_df["일자"].dt.strftime("%m/%d") == d)
            ]
            if len(sub) > 0 and sub["광고비"].sum() > 0:
                first_spend = d
                break
        is_new = first_spend and first_spend > all_dates[0]

        rows = []
        for d in recent5:
            sub = naver_df[
                (naver_df["디바이스"] == dev) & (naver_df["키워드"] == kw) &
                (naver_df["일자"].dt.strftime("%m/%d") == d)
            ]
            if len(sub) == 0:
                rows.append({"date": d, "spend": 0, "conv": 0, "cpa": 0, "cpc": 0, "rank": 0, "imp": 0})
                continue
            sp = int(sub["광고비"].sum()); cv = float(sub["총전환"].sum())
            cl = int(sub["클릭"].sum()); imp = int(sub["노출"].sum())
            rank = round(float(sub["평균노출순위"].mean()), 1)
            rows.append({
                "date": d, "spend": sp, "conv": round(cv, 1), "imp": imp,
                "cpa": int(round(sp / cv)) if cv > 0 else 0,
                "cpc": int(round(sp / cl)) if cl > 0 else 0,
                "rank": rank,
            })
        return {"rows": rows, "is_new": is_new, "start": first_spend}

    auto_data = {dev: {kw: auto_kw_rows(dev, kw) for kw in AUTO_BID_KEYWORDS} for dev in ["MO", "PC"]}

    def sec(df, kw_df):
        return {
            "curr": _kpi(df, curr), "prev": _kpi(df, prev),
            "wd_avg": wd_avg(df),
            "kw": _kw_day(kw_df, curr),
            "kw_wd_avg": _kw_wd_avg(kw_df, recent_wd),
        }

    return {
        "curr": curr, "curr_day": meta["curr_day"],
        "prev": prev, "prev_day": meta["prev_day"],
        "data_range": f"{meta['data_start']}~{meta['data_end']}",
        "n_pc":    sec(naver_pc, naver_pc),
        "n_mo":    sec(naver_mo, naver_mo),
        "g_brand": {"curr": _kpi(google_brand, curr), "prev": _kpi(google_brand, prev), "kw": _kw_day(google_brand, curr)},
        "g_comp":  {"curr": _kpi(google_comp,  curr), "prev": _kpi(google_comp,  prev), "kw": _kw_day(google_comp,  curr)},
        "g_gen":   {"curr": _kpi(google_gen,   curr), "prev": _kpi(google_gen,   prev), "kw": _kw_day(google_gen,   curr)},
        "auto": auto_data,
    }


def generate_comments(cd: dict) -> dict:
    """규칙 기반 섹션별 코멘트 생성 (API 불필요)"""

    # ── 공통 헬퍼 ──
    def ko(v):
        """금액 한글 표기: 10만 이상이면 만원 단위"""
        return f"{v / 10000:.1f}만원" if v >= 100000 else f"{v:,}원"

    def cpa_vs(curr_cpa, prev_cpa):
        if prev_cpa == 0:
            return "전일 데이터 없음"
        return f"전일({prev_cpa:,}원) 대비 {'개선' if curr_cpa < prev_cpa else '상승'}"

    def goal(cpa):
        if cpa == 0:
            return "전환 0건"
        return f"목표({TARGET_CPA:,}원) {'이내' if cpa <= TARGET_CPA else '초과'}"

    def pdiff(curr_v, base_v):
        if not base_v:
            return None
        return round((curr_v - base_v) / base_v * 100)

    def ins(*lines):
        return "\n".join(f'    <div class="ins-i">{l}</div>' for l in lines if l)

    sections = {}

    # ─────────────────────────────────────
    # 네이버 PC
    # ─────────────────────────────────────
    s = cd["n_pc"]
    c, p, wa, kw, kwa = s["curr"], s["prev"], s["wd_avg"], s["kw"], s.get("kw_wd_avg", {})

    # 줄1: KPI + 전일 대비 + 목표
    l1 = f"광고비 {ko(c['spend'])}, 전환 {c['conv']}건, CPA {c['cpa']:,}원 — {cpa_vs(c['cpa'], p['cpa'])}. {goal(c['cpa'])}."

    # 줄2: 평일 대비 소진 + 저소진 키워드
    pct = pdiff(c['spend'], wa['spend'])
    if pct is not None:
        label = "저소진" if pct < 0 else "초과소진"
        l2 = f"PC 광고비 평일일평균({ko(wa['spend'])}) 대비 {pct:+d}% {label}."
        under = [(k, pdiff(k['spend'], kwa.get(k['kw'], 0)))
                 for k in kw if kwa.get(k['kw'], 0) > 0]
        under = sorted([(k, p2) for k, p2 in under if p2 is not None and p2 <= -30], key=lambda x: x[1])[:2]
        if under:
            parts = [f"[{k['kw']}] 평일 {ko(kwa[k['kw']])} → 당일 {ko(k['spend'])}({p2:+d}%)" for k, p2 in under]
            l2 += " " + ", ".join(parts) + "으로 주요 원인 확인."
    else:
        l2 = "평일일평균 비교 데이터 없음."

    # 줄3: 전환 발생 키워드
    conv_kws = [k for k in kw if k['conv'] > 0]
    if conv_kws:
        parts = [f"[{k['kw']}] CVR {k['cvr']}%, CPA {k['cpa']:,}원" for k in conv_kws[:3]]
        l3 = ", ".join(parts) + "으로 전환 연결."
    else:
        l3 = "당일 전환 발생 키워드 없음."

    sections["N_PC"] = ins(l1, l2, l3)

    # ─────────────────────────────────────
    # 네이버 MO
    # ─────────────────────────────────────
    s = cd["n_mo"]
    c, p, wa, kw, kwa = s["curr"], s["prev"], s["wd_avg"], s["kw"], s.get("kw_wd_avg", {})

    pct = pdiff(c['spend'], wa['spend'])
    pct_str = f" MO 광고비 평일일평균({ko(wa['spend'])}) 대비 {pct:+d}%{'  저소진' if pct and pct < 0 else ''}." if pct is not None else ""
    l1 = f"광고비 {ko(c['spend'])}, 전환 {c['conv']}건, CPA {c['cpa']:,}원 — {cpa_vs(c['cpa'], p['cpa'])}.{pct_str}"

    # 줄2: 주요 저소진 키워드 (광고비 많은 순으로)
    under = [(k, pdiff(k['spend'], kwa.get(k['kw'], 0)))
             for k in sorted(kw, key=lambda x: -x['spend']) if kwa.get(k['kw'], 0) > 0]
    under = [(k, p2) for k, p2 in under if p2 is not None and p2 <= -20][:3]
    if under:
        parts = [f"[{k['kw']}] 광고비 {ko(k['spend'])}, CPA {k['cpa']:,}원 — 평일일평균({ko(kwa[k['kw']])}) 대비 {p2:+d}%" for k, p2 in under[:2]]
        l2 = " ".join(parts) + "."
    else:
        l2 = "주요 키워드 소진 패턴 큰 변화 없음."

    # 줄3: 전환 0건 키워드
    zero_kws = [k for k in kw if k['conv'] == 0 and k['spend'] > 10000]
    if zero_kws:
        names = " ".join(f"[{k['kw']}]" for k in zero_kws[:4])
        l3 = f"{names} 전환 0건 지속."
    else:
        l3 = "전환 0건 지속 키워드 없음."

    # 줄4: 전환 발생 키워드
    conv_kws = [k for k in kw if k['conv'] > 0]
    if conv_kws:
        parts = [f"[{k['kw']}] 전환 {k['conv']}건(CPA {k['cpa']:,}원)" for k in conv_kws[:3]]
        l4 = ", ".join(parts) + "으로 전환 연결."
    else:
        l4 = "당일 전환 발생 키워드 없음."

    sections["N_MO"] = ins(l1, l2, l3, l4)

    # ─────────────────────────────────────
    # 구글 브랜드
    # ─────────────────────────────────────
    s = cd["g_brand"]
    c, p, kw = s["curr"], s["prev"], s["kw"]

    l1 = f"광고비 {ko(c['spend'])}, 전환 {c['conv']}건, CPA {c['cpa']:,}원 — {cpa_vs(c['cpa'], p['cpa'])}. {goal(c['cpa'])}."

    conv_kws = [k for k in kw if k['conv'] > 0]
    zero_kws = [k for k in kw if k['conv'] == 0 and k['spend'] > 0]
    parts = []
    if conv_kws:
        parts.append(", ".join(f"[{k['kw']}] CVR {k['cvr']}%, CPA {k['cpa']:,}원" for k in conv_kws[:2]) + "으로 전환 연결.")
    if zero_kws:
        parts.append(", ".join(f"[{k['kw']}] 광고비 {ko(k['spend'])} 소진, 전환 0건" for k in zero_kws[:2]) + ".")
    l2 = " ".join(parts) if parts else "전환 발생 키워드 없음."

    sections["G_BRAND"] = ins(l1, l2)

    # ─────────────────────────────────────
    # 구글 경쟁사
    # ─────────────────────────────────────
    s = cd["g_comp"]
    c, p, kw = s["curr"], s["prev"], s["kw"]

    l1 = f"광고비 {ko(c['spend'])}, 전환 {c['conv']}건, CPA {c['cpa']:,}원 — {cpa_vs(c['cpa'], p['cpa'])}. {goal(c['cpa'])}."

    conv_kws = sorted([k for k in kw if k['conv'] > 0], key=lambda x: x['cpa'])
    zero_kws = [k for k in kw if k['conv'] == 0 and k['spend'] > 5000]

    if conv_kws:
        parts = [f"[{k['kw']}] CPA {k['cpa']:,}원(전환 {k['conv']}건)" for k in conv_kws[:3]]
        l2 = ", ".join(parts) + "."
        if conv_kws:
            l2 += f" [{conv_kws[0]['kw']}]이 경쟁사 내 최고 효율."
    else:
        l2 = "전환 발생 키워드 없음."

    l3 = (", ".join(f"[{k['kw']}]" for k in zero_kws[:4]) + " 전환 0건 지속." if zero_kws
          else "전환 0건 지속 키워드 없음.")

    sections["G_COMP"] = ins(l1, l2, l3)

    # ─────────────────────────────────────
    # 구글 일반
    # ─────────────────────────────────────
    s = cd["g_gen"]
    c, p, kw = s["curr"], s["prev"], s["kw"]

    l1 = f"광고비 {ko(c['spend'])}, 전환 {c['conv']}건, CPA {c['cpa']:,}원 — {cpa_vs(c['cpa'], p['cpa'])}. {goal(c['cpa'])}."

    inefficient = sorted([k for k in kw if k['conv'] == 0 and k['spend'] > 20000], key=lambda x: -x['spend'])
    conv_kws = sorted([k for k in kw if k['conv'] > 0], key=lambda x: x['cpa'])

    l2 = (", ".join(f"[{k['kw']}] 광고비 {ko(k['spend'])} 소진, 전환 0건" for k in inefficient[:2]) + "." if inefficient
          else "전환 0건 고소진 키워드 없음.")

    l3 = (", ".join(f"[{k['kw']}] 전환 {k['conv']}건(CPA {k['cpa']:,}원)" for k in conv_kws[:3]) + "으로 전환 발생." if conv_kws
          else "당일 전환 발생 키워드 없음.")

    sections["G_GEN"] = ins(l1, l2, l3)

    # ─────────────────────────────────────
    # Summary
    # ─────────────────────────────────────
    npc = cd["n_pc"]; nmo = cd["n_mo"]
    gb = cd["g_brand"]; gc = cd["g_comp"]; gg = cd["g_gen"]

    def g_cpa_str(curr_cpa, prev_cpa, label):
        if curr_cpa == 0:
            return f"{label} 전환 0건"
        delta = '개선' if curr_cpa < prev_cpa else '상승'
        return f"{label} CPA {curr_cpa:,}원(전일 {prev_cpa:,}원 대비 {delta})"

    pct_pc = pdiff(npc["curr"]["spend"], npc["wd_avg"]["spend"])
    pct_mo = pdiff(nmo["curr"]["spend"], nmo["wd_avg"]["spend"])
    s1 = (f"네이버 SA PC: 광고비 {ko(npc['curr']['spend'])} / 전환 {npc['curr']['conv']}건 / CPA {npc['curr']['cpa']:,}원"
          f" — {cpa_vs(npc['curr']['cpa'], npc['prev']['cpa'])}."
          + (f" 평일일평균 대비 {pct_pc:+d}%." if pct_pc is not None else ""))
    s2 = (f"네이버 SA MO: 광고비 {ko(nmo['curr']['spend'])} / 전환 {nmo['curr']['conv']}건 / CPA {nmo['curr']['cpa']:,}원"
          f" — {cpa_vs(nmo['curr']['cpa'], nmo['prev']['cpa'])}."
          + (f" 평일일평균 대비 {pct_mo:+d}%." if pct_mo is not None else ""))

    # 줄3: 네이버 전체 저소진 키워드
    all_under = {}
    for sec_key in ["n_pc", "n_mo"]:
        sec_data = cd[sec_key]
        kwa = sec_data.get("kw_wd_avg", {})
        for k in sec_data["kw"]:
            wa_k = kwa.get(k["kw"], 0)
            if wa_k > 0:
                p2 = pdiff(k["spend"], wa_k)
                if p2 is not None and p2 < 0:
                    key = f"[{k['kw']}]"
                    if key not in all_under or p2 < all_under[key]:
                        all_under[key] = p2
    top_under = sorted(all_under.items(), key=lambda x: x[1])[:5]
    if top_under:
        kw_str = ", ".join(f"{name} {p2:+d}%" for name, p2 in top_under)
        s3 = f"주요 소진 변화 키워드 (평일일평균 대비): {kw_str}."
    else:
        s3 = "주요 키워드 소진 패턴 전일 대비 변화 없음."

    s4 = (f"구글 SA: {g_cpa_str(gb['curr']['cpa'], gb['prev']['cpa'], '브랜드')}, "
          f"{g_cpa_str(gc['curr']['cpa'], gc['prev']['cpa'], '경쟁사')}, "
          f"{g_cpa_str(gg['curr']['cpa'], gg['prev']['cpa'], '일반')}.")

    sections["SUMMARY"] = ins(s1, s2, s3, s4)

    # ─────────────────────────────────────
    # 자동입찰
    # ─────────────────────────────────────
    auto_html = []

    def auto_line(dev, kw_name):
        d = cd["auto"][dev][kw_name]
        rows = [r for r in d["rows"] if r["spend"] > 0]
        new_tag = f" ★신규({d['start']}~)" if d["is_new"] else ""
        tag = f"[{dev}] {kw_name}{new_tag}"

        if not rows:
            return f"<strong>{tag}</strong> — 해당 기간 데이터 없음."

        date_range = f"{rows[0]['date']}~{rows[-1]['date']}" if len(rows) > 1 else rows[0]['date']
        cpcs = [r['cpc'] for r in rows if r['cpc'] > 0]
        cpc_str = (f"CPC {min(cpcs):,}~{max(cpcs):,}원" if len(set(cpcs)) > 1 else f"CPC {cpcs[0]:,}원") if cpcs else ""

        total_conv = round(sum(r['conv'] for r in rows), 1)
        conv_rows = [r for r in rows if r['conv'] > 0]
        if conv_rows:
            avg_cpa = int(sum(r['cpa'] for r in conv_rows) / len(conv_rows))
            conv_str = f" 전환 {total_conv}건, CPA {avg_cpa:,}원({goal(avg_cpa)})."
        else:
            conv_str = " 전환 0건 지속."

        ranks = [r['rank'] for r in rows if r.get('rank', 0) > 0]
        rank_str = f" 노출순위 {round(sum(ranks)/len(ranks), 1)}위 유지." if ranks else ""

        body = f"{date_range}" + (f" {cpc_str}." if cpc_str else "") + conv_str + rank_str
        return f"<strong>{tag}</strong> — {body}"

    # MO 5개
    for kw in AUTO_BID_KEYWORDS:
        auto_html.append(f'    <div class="ins-i">{auto_line("MO", kw)}</div>')

    # PC 앞 3개 개별
    for kw in AUTO_BID_KEYWORDS[:3]:
        auto_html.append(f'    <div class="ins-i">{auto_line("PC", kw)}</div>')

    # PC 마지막 2개 묶음 (입주청소가격·입주청소전문)
    kw1, kw2 = AUTO_BID_KEYWORDS[3], AUTO_BID_KEYWORDS[4]
    d1, d2 = cd["auto"]["PC"][kw1], cd["auto"]["PC"][kw2]
    new_tag = ""
    if d1["is_new"] or d2["is_new"]:
        start = d1["start"] or d2["start"]
        new_tag = f" ★신규({start}~)"
    tag = f"[PC] {kw1}·{kw2}{new_tag}"
    all_rows = [r for r in d1["rows"] + d2["rows"] if r["spend"] > 0]
    if all_rows:
        ranks = [r['rank'] for r in all_rows if r.get('rank', 0) > 0]
        rank_str = f" PC 순위 {round(sum(ranks)/len(ranks), 1)}위로" if ranks else ""
        total_conv = round(sum(r['conv'] for r in all_rows), 1)
        conv_str = "클릭 및 전환 거의 미발생. PC에서의 효과는 제한적." if total_conv == 0 else f"전환 {total_conv}건 발생."
        pc_body = f"{rank_str} {conv_str}"
    else:
        pc_body = "해당 기간 데이터 없음."
    auto_html.append(f'    <div class="ins-i"><strong>{tag}</strong> — {pc_body}</div>')

    sections["AUTO"] = "\n".join(auto_html)

    return sections


def build_all_comments_json(naver_pc, naver_mo, google_brand, google_comp, google_gen,
                             naver_df, meta, all_dates):
    """전체 날짜별 코멘트 JSON 생성 (날짜 선택 필터용)"""
    year = meta["curr"].year
    day_ko = ["월","화","수","목","금","토","일"]
    result = {}

    for i, curr in enumerate(all_dates):
        prev = all_dates[i - 1] if i > 0 else curr

        # curr_day, prev_day 계산
        try:
            curr_dt = pd.Timestamp(f"{year}-{curr.replace('/', '-')}")
            prev_dt = pd.Timestamp(f"{year}-{prev.replace('/', '-')}")
            curr_day = day_ko[curr_dt.weekday()]
            prev_day = day_ko[prev_dt.weekday()]
        except Exception:
            curr_day = prev_day = ""

        # 해당 날짜 기준 recent_wd: curr 이전 평일 5일
        recent_wd = [
            d for d in all_dates[:i]
            if pd.Timestamp(f"{year}-{d.replace('/', '-')}").weekday() < 5
        ][-5:]

        def wd_avg_for(df):
            vals = [_kpi(df, d) for d in recent_wd]
            if not vals:
                return {"spend": 0, "conv": 0, "cpa": 0}
            sp = int(sum(v["spend"] for v in vals) / len(vals))
            cv = round(sum(v["conv"] for v in vals) / len(vals), 1)
            cpa_vals = [v["cpa"] for v in vals if v["cpa"] > 0]
            return {"spend": sp, "conv": cv, "cpa": int(sum(cpa_vals)/len(cpa_vals)) if cpa_vals else 0}

        def auto_kw_rows_for(dev, kw):
            # first_spend: 전체 기간에서 최초 광고비 발생일
            first_spend = None
            for d in all_dates:
                sub = naver_df[
                    (naver_df["디바이스"] == dev) & (naver_df["키워드"] == kw) &
                    (naver_df["일자"].dt.strftime("%m/%d") == d)
                ]
                if len(sub) > 0 and sub["광고비"].sum() > 0:
                    first_spend = d
                    break
            is_new = first_spend and first_spend > all_dates[0]
            # recent5: curr 포함 직전 5일
            recent5 = all_dates[max(0, i-4):i+1]
            rows = []
            for d in recent5:
                sub = naver_df[
                    (naver_df["디바이스"] == dev) & (naver_df["키워드"] == kw) &
                    (naver_df["일자"].dt.strftime("%m/%d") == d)
                ]
                if len(sub) == 0:
                    rows.append({"date": d, "spend": 0, "conv": 0, "cpa": 0, "cpc": 0, "rank": 0, "imp": 0})
                    continue
                sp = int(sub["광고비"].sum()); cv = float(sub["총전환"].sum())
                cl = int(sub["클릭"].sum()); imp = int(sub["노출"].sum())
                rank = round(float(sub["평균노출순위"].mean()), 1)
                rows.append({
                    "date": d, "spend": sp, "conv": round(cv, 1), "imp": imp,
                    "cpa": int(round(sp / cv)) if cv > 0 else 0,
                    "cpc": int(round(sp / cl)) if cl > 0 else 0,
                    "rank": rank,
                })
            return {"rows": rows, "is_new": is_new, "start": first_spend}

        def sec_for(df, kw_df):
            return {
                "curr": _kpi(df, curr), "prev": _kpi(df, prev),
                "wd_avg": wd_avg_for(df),
                "kw": _kw_day(kw_df, curr),
                "kw_wd_avg": _kw_wd_avg(kw_df, recent_wd),
            }

        cd = {
            "curr": curr, "curr_day": curr_day,
            "prev": prev, "prev_day": prev_day,
            "data_range": f"{all_dates[0]}~{all_dates[-1]}",
            "n_pc":    sec_for(naver_pc, naver_pc),
            "n_mo":    sec_for(naver_mo, naver_mo),
            "g_brand": {"curr": _kpi(google_brand, curr), "prev": _kpi(google_brand, prev), "kw": _kw_day(google_brand, curr)},
            "g_comp":  {"curr": _kpi(google_comp,  curr), "prev": _kpi(google_comp,  prev), "kw": _kw_day(google_comp,  curr)},
            "g_gen":   {"curr": _kpi(google_gen,   curr), "prev": _kpi(google_gen,   prev), "kw": _kw_day(google_gen,   curr)},
            "auto": {dev: {kw: auto_kw_rows_for(dev, kw) for kw in AUTO_BID_KEYWORDS} for dev in ["MO", "PC"]},
        }

        result[curr] = generate_comments(cd)

    return json.dumps(result, ensure_ascii=False)


# ══════════════════════════════════════════
# 7. 표(tbody) HTML 생성
# ══════════════════════════════════════════

def td(val, style="", bold=False):
    s = f' style="{style}"' if style else ""
    inner = f"<strong>{val}</strong>" if bold else str(val)
    return f"<td{s}>{inner}</td>"


def make_table_rows(df, dates, curr_str, is_naver=True):
    """일자별 tbody 행 생성"""
    rows_html = []
    for d in dates:
        sub = df[df["일자"].dt.strftime("%m/%d") == d]
        if len(sub) == 0:
            continue
        imp   = int(sub["노출"].sum())
        clk   = int(sub["클릭"].sum())
        spend = int(sub["광고비"].sum())
        conv  = sub["총전환"].sum()
        rank_col = sub["평균노출순위"].mean() if "평균노출순위" in sub.columns else None

        ctr = round(clk/imp*100, 3) if imp > 0 else 0
        cpc = int(round(spend/clk, 0)) if clk > 0 else 0
        cvr = round(conv/clk*100, 2) if clk > 0 else 0
        cpa = int(round(spend/conv, 0)) if conv > 0 else 0

        is_curr = (d == curr_str)
        cls = ' class="tnew-g"' if (is_curr and is_naver) else (' class="tnew"' if is_curr else "")

        # CPA 색상
        cpa_style = ""
        if cpa > 0 and cpa <= TARGET_CPA:
            cpa_style = "color:var(--gnt);font-weight:700"
        elif cpa > TARGET_CPA * 1.5:
            cpa_style = "color:var(--rd);font-weight:700"

        date_label = f"{d} ★" if is_curr else d
        cells = [
            td(f"<strong>{date_label}</strong>" if is_curr else date_label),
            td(f"{imp:,}"),
            td(str(clk)),
            td(f"<strong>{spend:,}</strong>" if is_curr else f"{spend:,}"),
            td(f"{ctr}%"),
            td(f"{cpc:,}"),
            td(f"<strong>{round(conv,1)}건</strong>" if is_curr else f"{round(conv,1)}건"),
            td(f"{cvr}%"),
            td(f"<strong>{cpa:,}</strong>" if is_curr else f"{cpa:,}", style=cpa_style),
        ]
        if rank_col is not None and not np.isnan(rank_col):
            cells.append(td(f"{round(rank_col,2)}위"))

        rows_html.append(f"      <tr{cls}>{''.join(cells)}</tr>")

    return "\n".join(rows_html)


def make_google_table_rows(df, dates, curr_str):
    """구글 SA tbody 행 (노출순위 없음)"""
    rows_html = []
    for d in dates:
        sub = df[df["일자"].dt.strftime("%m/%d") == d]
        if len(sub) == 0:
            continue
        imp   = int(sub["노출"].sum())
        clk   = int(sub["클릭"].sum())
        spend = int(sub["광고비"].sum())
        conv  = sub["총전환"].sum()

        ctr = round(clk/imp*100, 3) if imp > 0 else 0
        cpc = int(round(spend/clk, 0)) if clk > 0 else 0
        cvr = round(conv/clk*100, 2) if clk > 0 else 0
        cpa = int(round(spend/conv, 0)) if conv > 0 else 0

        is_curr = (d == curr_str)
        cls = ' class="tnew"' if is_curr else ""

        cpa_style = ""
        if cpa > 0 and cpa <= TARGET_CPA:
            cpa_style = "color:var(--gnt);font-weight:700"
        elif cpa > TARGET_CPA * 1.5:
            cpa_style = "color:var(--rd);font-weight:700"

        date_label = f"{d} ★" if is_curr else d
        cells = [
            td(f"<strong>{date_label}</strong>" if is_curr else date_label),
            td(f"{imp:,}"),
            td(str(clk)),
            td(f"<strong>{spend:,}</strong>" if is_curr else f"{spend:,}"),
            td(f"{ctr}%"),
            td(f"{cpc:,}"),
            td(f"<strong>{round(conv,1)}건</strong>" if is_curr else f"{round(conv,1)}건"),
            td(f"{cvr}%"),
            td(f"<strong>{cpa:,}</strong>" if is_curr else f"{cpa:,}", style=cpa_style),
        ]
        rows_html.append(f"      <tr{cls}>{''.join(cells)}</tr>")
    return "\n".join(rows_html)


# ══════════════════════════════════════════
# 7. 플레이스홀더 치환
# ══════════════════════════════════════════

def fill_template(template: str, replacements: dict) -> str:
    for key, val in replacements.items():
        template = template.replace("{{" + key + "}}", str(val))
    return template


# ══════════════════════════════════════════
# 8. 메인
# ══════════════════════════════════════════

def main():
    print("=" * 55)
    print("  크린토피아 Daily Report 자동 빌드")
    print("=" * 55)

    # ── 데이터 로드 ──
    print("\n[1] 구글 시트 로드 중...")
    raw_df     = load_sheet(SHEET_MEDIA_RAW)
    summary_df = load_sheet(SHEET_SUMMARY)

    df = prepare_media_raw(raw_df)

    # ── 날짜 메타 ──
    print("\n[2] 날짜 분석 중...")
    meta = get_date_meta(df)
    curr_str = meta["curr_str"]
    prev_str = meta["prev_str"]
    all_dates = meta["all_dates"]
    recent7   = meta["recent7"]
    n_total   = len(all_dates)

    print(f"  📅 최신일: {curr_str} ({meta['curr_day']})")
    print(f"  📅 전일:   {prev_str} ({meta['prev_day']})")
    print(f"  📊 전체 데이터: {all_dates[0]} ~ {all_dates[-1]} ({n_total}일)")

    # ── 매체별 분리 ──
    naver  = df[df["매체"] == "네이버 SA"]
    google = df[df["매체"] == "구글 SA"]
    naver_pc = naver[naver["디바이스"] == "PC"]
    naver_mo = naver[naver["디바이스"] == "MO"]

    # ── KPI 계산 ──
    print("\n[3] KPI 계산 중...")
    n_curr = daily_kpi(naver, curr_str)
    n_prev = daily_kpi(naver, prev_str)
    g_curr = daily_kpi(google, curr_str)
    g_prev = daily_kpi(google, prev_str)

    n_spend_txt, n_spend_cls = diff_badge(n_curr["spend"], n_prev["spend"])
    n_cpa_txt,   n_cpa_cls   = diff_badge(n_curr["cpa"],   n_prev["cpa"],   reverse=True)
    g_spend_txt, g_spend_cls = diff_badge(g_curr["spend"], g_prev["spend"])
    g_cpa_txt,   g_cpa_cls   = diff_badge(g_curr["cpa"],   g_prev["cpa"],   reverse=True)

    print(f"  네이버 CPA: {int(n_curr['cpa']):,}원  /  구글 CPA: {int(g_curr['cpa']):,}원")

    # ── 차트 데이터 ──
    print("\n[4] 차트 데이터 생성 중...")
    summary_data = parse_summary_sheet(summary_df)
    chart = build_chart_data(naver, google, all_dates, summary_data)

    # ── 전체 날짜별 KPI JSON ──
    google_brand = google[google["구글유형"] == "브랜드"]
    google_comp  = google[google["구글유형"] == "경쟁사"]
    google_gen   = google[google["구글유형"] == "일반"]
    all_kpi_json = build_all_daily_kpi_json(naver, naver_pc, naver_mo, google, google_brand, google_comp, google_gen, all_dates)

    # ── 자동입찰 JS ──
    print("\n[5] 자동입찰 데이터 생성 중...")
    auto_js = build_auto_detail_js(naver, all_dates)

    # ── 코멘트 생성 ──
    print("\n[6] 코멘트 생성 중...")
    comment_data = build_comment_data(
        naver_pc, naver_mo, google_brand, google_comp, google_gen,
        naver, meta, all_dates
    )
    comments = generate_comments(comment_data)
    all_comments_json = build_all_comments_json(
        naver_pc, naver_mo, google_brand, google_comp, google_gen,
        naver, meta, all_dates
    )
    # 자동입찰 제목 (신규 키워드 자동 감지)
    new_kws = []
    for kw in AUTO_BID_KEYWORDS:
        d = comment_data["auto"]["MO"][kw]
        if d["is_new"]:
            new_kws.append(f"{kw}({d['start']}~)")
    title_auto = f"네이버 자동입찰 키워드 인사이트 — {meta['data_start']}~{meta['data_end']}"
    if new_kws:
        title_auto += f" ({', '.join(new_kws)} 신규 추가)"

    # ── 표 HTML ──
    print("\n[7] 표 데이터 생성 중...")
    table_n_pc = make_table_rows(naver_pc, recent7, curr_str, is_naver=True)
    table_n_mo = make_table_rows(naver_mo, recent7, curr_str, is_naver=True)
    table_g    = make_google_table_rows(google, recent7, curr_str)

    # ── JS 날짜 배열 ──
    js_l7  = json.dumps(recent7, ensure_ascii=False)
    js_pl  = json.dumps(all_dates, ensure_ascii=False)
    n7  = len(recent7)
    npl = n_total

    ncol  = f'[...Array({n7-1}).fill("rgba(15,158,110,0.55)"),"rgba(15,158,110,0.9)"]'
    gcol  = f'[...Array({n7-1}).fill("rgba(45,125,210,0.55)"),"rgba(45,125,210,0.9)"]'
    ncolp = f'[...Array({npl-1}).fill("rgba(15,158,110,0.55)"),"rgba(15,158,110,0.9)"]'
    gcolp = f'[...Array({npl-1}).fill("rgba(45,125,210,0.55)"),"rgba(45,125,210,0.9)"]'

    # ── 템플릿 치환 ──
    print("\n[8] 템플릿 빌드 중...")
    tpl_path = os.path.join(BASE_DIR, TEMPLATE_FILENAME)
    if not os.path.exists(tpl_path):
        print(f"  ❌ {TEMPLATE_FILENAME} 파일이 없습니다!")
        sys.exit(1)
    with open(tpl_path, encoding="utf-8") as f:
        template = f.read()

    replacements = {
        # 날짜
        "DATA_RANGE":       f"{meta['data_start']}~{meta['data_end']}",
        "COMPARE_PERIOD":   f"{prev_str} vs {curr_str}",
        "UPDATE_DATE":      meta["update_str"],
        "CURR_DATE":        curr_str,
        "SUMMARY_TITLE":    f"{meta['curr_label']} ({meta['curr_day']}) 요약 — 전일({prev_str} {meta['prev_day']}) 대비",

        # KPI 카드
        "N_SPEND":      fmt_spend(n_curr["spend"]),
        "N_SPEND_DIFF": n_spend_txt,
        "N_SPEND_CLS":  n_spend_cls,
        "N_CPA":        f"{int(n_curr['cpa']):,}원",
        "N_CPA_DIFF":   n_cpa_txt,
        "N_CPA_CLS":    n_cpa_cls,
        "G_SPEND":      fmt_spend(g_curr["spend"]),
        "G_SPEND_DIFF": g_spend_txt,
        "G_SPEND_CLS":  g_spend_cls,
        "G_CPA":        f"{int(g_curr['cpa']):,}원",
        "G_CPA_DIFF":   g_cpa_txt,
        "G_CPA_CLS":    g_cpa_cls,

        # JS 날짜/색상 배열
        "JS_L7":    js_l7,
        "JS_L15":   js_pl,   # L15 변수명이지만 실제는 전체 기간
        "JS_PL":    js_pl,
        "JS_NCOL":  ncol,
        "JS_GCOL":  gcol,
        "JS_NCOLP": ncolp,
        "JS_GCOLP": gcolp,

        # 차트 데이터
        **{k: v for k, v in chart.items()},

        # 자동입찰
        "AUTO_DETAIL_JS": auto_js,

        # 날짜 선택 필터
        "ALL_DAILY_KPI_JSON": all_kpi_json,
        "TARGET_CPA":  TARGET_CPA,
        "DATA_YEAR":   meta["curr"].year,

        # 표
        "TABLE_N_PC_SUMMARY": table_n_pc,
        "TABLE_N_MO_SUMMARY": table_n_mo,
        "TABLE_G_SUMMARY":    table_g,

        # 섹션 제목
        "TITLE_N_PC":   f"네이버 SA · PC — {curr_str} ({meta['curr_day']}) 전일({prev_str} {meta['prev_day']}) 대비",
        "TITLE_N_MO":   f"네이버 SA · MO — {curr_str} ({meta['curr_day']}) 전일({prev_str} {meta['prev_day']}) 대비",
        "TITLE_G_BRAND":f"구글 SA · 브랜드 — {curr_str} ({meta['curr_day']}) 전일({prev_str} {meta['prev_day']}) 대비",
        "TITLE_G_COMP": f"구글 SA · 경쟁사 — {curr_str} ({meta['curr_day']}) 전일({prev_str} {meta['prev_day']}) 대비",
        "TITLE_G_GEN":  f"구글 SA · 일반 — {curr_str} ({meta['curr_day']}) 전일({prev_str} {meta['prev_day']}) 대비",
        "TITLE_AUTO":   title_auto,

        # 코멘트
        "COMMENT_SUMMARY": comments.get("SUMMARY", ""),
        "COMMENT_N_PC":    comments.get("N_PC", ""),
        "COMMENT_N_MO":    comments.get("N_MO", ""),
        "COMMENT_G_BRAND": comments.get("G_BRAND", ""),
        "COMMENT_G_COMP":  comments.get("G_COMP", ""),
        "COMMENT_G_GEN":   comments.get("G_GEN", ""),
        "COMMENT_AUTO":    comments.get("AUTO", ""),

        # 전체 날짜 코멘트 JSON (날짜 선택 필터용)
        "ALL_COMMENTS_JSON": all_comments_json,
    }

    output = fill_template(template, replacements)

    # 미채워진 플레이스홀더 경고
    remaining = re.findall(r"\{\{[A-Z_]+\}\}", output)
    if remaining:
        print(f"  ⚠️  미채워진 플레이스홀더: {set(remaining)}")

    # ── 저장 ──
    date_tag = meta["curr"].strftime("%y%m%d")
    out_name = OUTPUT_FILENAME.format(date=date_tag)
    out_path = os.path.join(BASE_DIR, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(output)

    print(f"\n{'='*55}")
    print(f"  ✅ 완료! → {out_name}")
    print(f"{'='*55}")
    print(f"  브라우저에서 파일을 열어 확인하세요.")


if __name__ == "__main__":
    main()
