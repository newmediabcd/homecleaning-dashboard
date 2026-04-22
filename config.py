# ============================================================
# 크린토피아 홈클리닝 Daily Report 설정
# ============================================================

# 구글 시트 ID (URL에서 /d/ 뒤 ~ /edit 앞 부분)
SHEET_ID = "1pVvSS__AFoxkf2qu9K5DqA09LhO3byTiQJtwaLzu84w"

# 시트 이름
SHEET_MEDIA_RAW   = "media raw"
SHEET_SUMMARY     = "전체_summary"
SHEET_GA4         = "GA4 raw"

# 출력 파일명 (template.html과 같은 폴더)
OUTPUT_FILENAME = "크린토피아_DailyReport_대시보드_{date}.html"
TEMPLATE_FILENAME = "template.html"

# 구글 SA 캠페인 유형 판별 키워드
GOOGLE_TYPE_RULES = {
    "경쟁사": "_경쟁사_",
    "브랜드": "_브랜드_",
    "일반":   None,   # 위 둘 다 아닌 경우
}

# 네이버 SA 디바이스 판별
NAVER_PC_KEYWORD = "_ PC"   # 캠페인명에 이 문자열 포함 → PC

# 자동입찰 키워드 목록 (순서 유지)
AUTO_BID_KEYWORDS = ["입주청소", "이사청소", "입주청소비용", "입주청소가격", "입주청소전문"]
# ※ 입주청소가격, 입주청소전문은 4/17부터 추가 (데이터 없는 날은 0으로 처리)

# 목표 CPA
TARGET_CPA = 30000

# 차트 색상
COLOR_NAVER_BAR    = "rgba(15,158,110,{a})"   # 초록
COLOR_GOOGLE_BAR   = "rgba(45,125,210,{a})"   # 파란
COLOR_CPA_LINE     = "rgba(217,64,64,0.85)"
COLOR_CAC_LINE     = "rgba(176,109,16,0.85)"
COLOR_GA_LINE      = "rgba(217,64,64,0.85)"
