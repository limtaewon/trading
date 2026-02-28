#!/bin/bash
# ============================================================
# enrich_data.sh - OpenClaw 보조 데이터 강화 오케스트레이터
#
# OpenClaw gpt-5.2의 매매 판단 품질을 극대화하기 위해
# 기술 지표, 시장 레짐, 공시 데이터를 사전 계산/수집한다.
#
# 사용법:
#   bash ~/.openclaw/scripts/trading/enrich_data.sh           # 전체 실행
#   bash ~/.openclaw/scripts/trading/enrich_data.sh --quick   # 시장레짐만 (빠른)
#   bash ~/.openclaw/scripts/trading/enrich_data.sh --tech    # 기술지표만
#   bash ~/.openclaw/scripts/trading/enrich_data.sh --dart    # DART만
#
# 크론 추가 예시 (장전 07:30 + 장중 12:00):
#   30 7 * * 1-5 bash ~/.openclaw/scripts/trading/enrich_data.sh >> ~/.openclaw/logs/enrich.log 2>&1
#   0 12 * * 1-5 bash ~/.openclaw/scripts/trading/enrich_data.sh --quick >> ~/.openclaw/logs/enrich.log 2>&1
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$HOME/.openclaw/logs"
mkdir -p "$LOG_DIR"

MODE="${1:-all}"

echo "============================================================"
echo "$(date '+%Y-%m-%d %H:%M:%S') [보조 데이터 강화] 시작 (mode: $MODE)"
echo "============================================================"

# ─── 1. 서비스 확인 ──────────────────────────────────────────
echo ""
echo "▸ 서비스 상태 확인..."

CH_OK=false
if curl -sf "http://localhost:8123/ping" > /dev/null 2>&1; then
    echo "  ClickHouse: OK"
    CH_OK=true
else
    echo "  ClickHouse: FAIL → 중단"
    exit 1
fi

# ─── 2. 시장 데이터 최신화 ────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--quick" ]]; then
    echo ""
    echo "▸ 시장 데이터 수집..."
    python3 "$SCRIPT_DIR/collect_market_data.py" --days 7 2>&1 | tail -5
    echo "  완료"
fi

# ─── 3. 기술적 지표 계산 ──────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--tech" ]]; then
    echo ""
    echo "▸ 기술적 지표 계산 (핵심+뉴스/공시 전종목)..."
    python3 "$SCRIPT_DIR/technical_indicators.py" --dynamic 2>&1 | tail -15
    echo "  완료"
fi

# ─── 4. 시장 레짐 분류 ────────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--quick" ]]; then
    echo ""
    echo "▸ 시장 레짐 분류..."
    python3 "$SCRIPT_DIR/market_regime.py" 2>&1 | tail -10
    echo "  완료"
fi

# ─── 5. DART 공시 수집 ────────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--dart" ]]; then
    if [[ -n "${DART_API_KEY:-}" ]]; then
        echo ""
        echo "▸ DART 공시 수집..."
        python3 "$SCRIPT_DIR/collect_dart.py" --days 3 2>&1 | tail -10
        echo "  완료"
    else
        echo ""
        echo "▸ DART: API키 미설정, 스킵"
        echo "  발급: https://opendart.fss.or.kr/ → 인증키 신청"
        echo "  설정: export DART_API_KEY='발급받은키'"
    fi
fi

# ─── 6. 정규화 수급 동기화 ─────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--quick" ]]; then
    echo ""
    echo "▸ 정규화 수급 동기화..."
    python3 "$SCRIPT_DIR/sync_normalized_flow_daily.py" --days 14 2>&1 | tail -10
    echo "  완료"
fi

# ─── 7. Decision Operating(P0) 로그 생성 ─────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--quick" ]]; then
    echo ""
    echo "▸ 의사결정 파이프라인(P0) 실행..."
    python3 "$SCRIPT_DIR/decision_operating_pipeline.py" --horizon INTRADAY --universe watchlist --limit 30 2>&1 | tail -10
    echo "  완료"
fi

# ─── 8. 결과 요약 ─────────────────────────────────────────────
echo ""
echo "============================================================"
echo "$(date '+%Y-%m-%d %H:%M:%S') [보조 데이터 강화] 완료"
echo ""
echo "▸ OpenClaw gpt-5.2 전용 쿼리:"
echo "  SELECT * FROM trading.v_trading_dashboard    -- 종합 대시보드"
echo "  SELECT * FROM trading.v_stock_signals        -- 종목별 시그널"
echo "  SELECT * FROM trading.v_regime               -- 시장 레짐"
echo "  SELECT * FROM trading.v_recent_disclosures   -- 최근 공시"
echo "  SELECT * FROM trading.decision_run ORDER BY decision_time DESC LIMIT 5"
echo "  SELECT * FROM trading.decision_candidate WHERE decision_id='...'"
echo "============================================================"

# ─── 9. 텔레그램 요약 ─────────────────────────────────────────────
python3 "$SCRIPT_DIR/telegram_notify.py" "📊 <b>보조 데이터 강화 완료</b> (${MODE})
$(date '+%H:%M') | 시장데이터+기술지표+레짐+DART
→ gpt-5.2 HEARTBEAT 준비 완료" 2>/dev/null || true
