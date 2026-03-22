@echo off
REM PADO 전체 파이프라인 (15:35 트리거)
REM OHLCV갱신 → 캐시 → 차트 → 거래량 → 시황 → 재료 → 교집합 → 파동 → 뉴스

cd /d C:\Coding\PADO
call .venv\Scripts\activate.bat
echo [%date% %time%] 파이프라인 시작
python main.py --scan
echo [%date% %time%] 파이프라인 완료
