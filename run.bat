@echo off
echo =====================================
echo  크린토피아 Daily Report 빌드 시작
echo =====================================
cd /d "%~dp0"

:: Anthropic API 키 설정 (코멘트 자동 생성용)
:: 아래 YOUR_API_KEY_HERE 부분을 실제 키로 교체하세요
if "%ANTHROPIC_API_KEY%"=="" (
    set ANTHROPIC_API_KEY=YOUR_API_KEY_HERE
)

python -X utf8 build_dashboard.py
echo.
pause
