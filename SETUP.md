# PADO 설치 & 실행 가이드

## 1. 가상환경 생성

```powershell
# PADO 폴더로 이동
cd C:\Coding\PADO

# 가상환경 생성
python -m venv .venv

# 활성화 (PowerShell)
.\.venv\Scripts\Activate.ps1

# 활성화 (CMD)
.\.venv\Scripts\activate.bat

# 프롬프트에 (.venv) 표시되면 성공
```

## 2. 패키지 설치

```powershell
# 가상환경 활성화된 상태에서
pip install -r requirements.txt
```

## 3. .env 설정

```powershell
# 템플릿 복사
copy .env.example .env

# .env 편집 (메모장 또는 VSCode)
notepad .env
```

최소 필수 키:
```
KIWOOM_APPKEY=실제값
KIWOOM_SECRETKEY=실제값
DISCORD_WEBHOOK_CB=실제값
DISCORD_WEBHOOK_PADO=실제값
```

나머지 (DART, NAVER, GEMINI)는 없어도 돌아감 — 해당 기능만 스킵됨.

## 4. 테스트 실행

```powershell
# DB 초기화 + 차트 스캔 테스트
python main.py --scan

# 파동 스캔 테스트
python main.py --wave

# ClosingBell 테스트 (키움 API 필요)
python main.py --cb-pick

# 전체 1회 실행
python main.py --once
```

## 5. 스케줄러 실행 (실전)

```powershell
# 가상환경 활성화 후
python main.py

# 08:30 브리핑 / 14:00 눌림목 / 15:00 CB / 15:40 파이프라인
# Ctrl+C로 종료
```

## 6. VSCode 연동

`.vscode/settings.json`:
```json
{
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/Scripts/python.exe"
}
```

## 7. 폴더 최종 상태

```
C:\Coding\PADO\
├── .venv\              ← 가상환경 (git 제외)
├── .env                ← API 키 (git 제외)
├── .gitignore
├── main.py
├── config.py
├── requirements.txt
├── shared\
├── jaechageosi\
├── ...
└── data\
    └── pado.db         ← 첫 실행 시 자동 생성
```

## 8. .gitignore

```
.venv/
.env
data/pado.db
data/logs/
data/performance/
__pycache__/
*.pyc
```
