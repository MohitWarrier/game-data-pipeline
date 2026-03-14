# Game Pulse — all commands
# Usage: make <target>
#
# To install make: choco install make (run as admin)
# If you don't have make, run the commands under each target directly.

# --- Default: show available commands ---
help:
	@echo ""
	@echo "  Game Pulse — available commands:"
	@echo "  ================================="
	@echo ""
	@echo "  make start        Start pipeline + dashboard (the main command)"
	@echo "  make run          Run pipeline once"
	@echo "  make dashboard    Launch dashboard only"
	@echo "  make test         Run full test suite"
	@echo "  make test-smoke   Quick smoke test (~30s)"
	@echo "  make validate     Run 56 data quality checks"
	@echo "  make reset        Wipe database and start fresh"
	@echo "  make db-counts    Show row counts for all tables"
	@echo "  make logs         Show recent pipeline logs"
	@echo ""

# --- The Main Command ---

# Start everything: pipeline (runs every 30 min) + dashboard (live).
# One command. Press Ctrl+C to stop both.
start:
	python -c "\
	import subprocess, sys, signal, os; \
	os.makedirs('logs', exist_ok=True); \
	procs = [ \
	  subprocess.Popen([sys.executable, 'pipelines/pipeline.py', '--serve']), \
	  subprocess.Popen([sys.executable, '-m', 'streamlit', 'run', 'dashboard/app.py']), \
	]; \
	signal.signal(signal.SIGINT, lambda *_: [p.terminate() for p in procs]); \
	[p.wait() for p in procs]"

# --- Pipeline ---

# Run the pipeline once (ingest + dbt + validate)
run:
	python pipelines/pipeline.py

# Start scheduled pipeline only (no dashboard)
serve:
	python pipelines/pipeline.py --serve

# --- Dashboard ---

# Launch dashboard only (no pipeline)
dashboard:
	streamlit run dashboard/app.py

# --- Testing ---

# Quick smoke test (~30s): config + twitch + dbt + validate
test-smoke:
	python -m tests.test_pipeline --test smoke

# Full test (~2min): every component + integration
test:
	python -m tests.test_pipeline --test full

# First-run test: wipes DB, runs from scratch, restores DB
test-fresh:
	python -m tests.test_pipeline --test fresh

# All tests: full + first-run
test-all:
	python -m tests.test_pipeline --test all

# Run validation checks only (no ingest, just check existing data)
validate:
	python -m tests.validate_pipeline --verbose

# Validate with relaxed freshness
validate-relaxed:
	python -m tests.validate_pipeline --verbose --max-hours 24

# --- Database ---

# Wipe the database and run reports. Fresh start.
reset:
	python -c "\
	import os, shutil; \
	[os.remove(f) for f in ['data/game_pulse.duckdb', 'data/game_pulse.duckdb.wal'] if os.path.exists(f)]; \
	shutil.rmtree('logs/runs', ignore_errors=True); \
	os.makedirs('logs/runs', exist_ok=True); \
	open('logs/pipeline.log', 'w').close(); \
	print('Database wiped. Logs cleared. Ready for fresh start.')"

# Show row counts for all tables
db-counts:
	python -c "\
	import duckdb; \
	con = duckdb.connect('data/game_pulse.duckdb'); \
	tables = [r[0] for r in con.execute('SHOW TABLES').fetchall()]; \
	[print(f'{t}: {con.execute(f\"SELECT COUNT(*) FROM {t}\").fetchone()[0]} rows') for t in tables]; \
	con.close()"

# --- Individual Steps ---

ingest:
	python -m ingest.fetch_twitch
	python -m ingest.fetch_igdb
	python -m ingest.fetch_steam

dbt:
	dbt run --project-dir game_pulse
	dbt test --project-dir game_pulse

# --- Storage ---

archive-dry-run:
	python -m maintenance.archive --dry-run

archive:
	python -m maintenance.archive

# --- Debugging ---

logs:
	tail -50 logs/pipeline.log

.PHONY: help start run serve dashboard test-smoke test test-fresh test-all validate validate-relaxed reset db-counts ingest dbt archive-dry-run archive logs
