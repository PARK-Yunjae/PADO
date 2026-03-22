@echo off
REM PADO 아침 브리핑 (08:25 트리거)
REM 전일 스캔 결과 + 파동 알림 웹훅 발송

cd /d C:\Coding\PADO
call .venv\Scripts\activate.bat
echo [%date% %time%] 아침 브리핑 시작
python main.py --briefing
echo [%date% %time%] 아침 브리핑 완료
