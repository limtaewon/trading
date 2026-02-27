# Active Stock Trading Scripts

## Core execution
- codex_cron_router.sh
- codex_brain.sh
- prepare_gpt_prompt.py
- execute_gpt_orders.py
- trading_preflight.sh

## News / event pipeline
- collect_news.py
- monitor_news.py
- cluster_news.py
- llm_relation_reasoner.py
- analyze_news_research.py
- codex_exec_guard.py
- ticker_mapper.py
- _requests_compat.py

## Market / feature enrichment
- collect_market_data.py
- collect_dart.py
- technical_indicators.py
- market_regime.py
- enrich_data.sh
- refresh_stocks.py
- refresh_interest_watchlist.py

## Reporting / briefing
- morning_briefing.py
- market_briefing.py
- market_realtime.py
- send_dooray_briefing.py
- stock_rag_report_api.py

## Backfill / schemas
- run_news_backfill_2025.sh
- run_news_backfill_month.sh
- run_news_backfill_2025_all_months.sh
- run_news_backfill_2025_resume_09_12.sh
- schema_news_research.sql
- trading_response_schema.json
