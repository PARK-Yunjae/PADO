@echo off
REM ============================================
REM PADO 자동 실행 (Windows Task Scheduler용)
REM ============================================
REM 거래일: 전체 파이프라인 (--once)
REM 비거래일: 뉴스 수집 + 글로벌만 (--weekend)
REM
REM 윈도우 작업 스케줄러 설정:
REM   트리거: 매일 08:20 (PC 자동 전원 08:15 설정 권장)
REM   작업: 이 bat 파일 실행
REM   조건: "컴퓨터의 AC 전원이 켜져 있을 때만" 해제
REM ============================================

cd /d C:\Coding\PADO

REM 가상환경 활성화
call .venv\Scripts\activate.bat

REM 자동 판단: --once는 거래일이면 전체, 비거래일이면 뉴스만
echo [%date% %time%] PADO 시작
python main.py --once

echo [%date% %time%] PADO 종료
