# 보안 정리 체크리스트

아래 항목은 과거 커밋에 노출되었던 가능성이 있는 값을 기준으로 즉시 교체를 권장한다.

## 1) 즉시 회전(rotate) 권장
- Naver API: `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
- Telegram Bot: `TELEGRAM_BOT_TOKEN` (필수), 필요 시 채팅 접근 권한 재점검
- ClickHouse 계정 비밀번호: `CLICKHOUSE_PASS` (실운영 비밀번호를 사용 중이었다면 필수)

## 2) 로컬 환경 변수 재설정
- `cp .env.example .env`
- `.env`에 실제 값 입력
- 런타임에 `.env`가 로드되도록 배포 환경(launchd/cron) 점검

## 3) 히스토리 정리
- 민감값 제거 커밋 이후 브랜치 히스토리를 재작성하고 `force push` 수행
- 로컬/원격 캐시가 남아 있을 수 있으므로, 이미 복제된 저장소가 있다면 재클론 권장

## 4) 재발 방지
- 코드에 키/토큰 하드코딩 금지
- 신규 비밀값은 `.env` 또는 시크릿 매니저만 사용
- PR/커밋 전 `rg` 또는 시크릿 스캐너로 점검

