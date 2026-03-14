# Game Pulse — common commands
# Usage: make <target>
#
# What is this file?
#   A Makefile is a task runner. Instead of memorising long commands, you
#   type short names like "make run" or "make validate". The 'make' program
#   reads this file, finds the target you asked for, and runs the shell
#   command underneath it. That's all it does.
#
#   To use it you need 'make' installed (comes with Git Bash, or:
#   choco install make). If you don't have it, just run the commands
#   listed under each target directly — they're normal shell commands.

# --- Pipeline ---

# Run the full pipeline once (ingest + dbt + tests)
run:
	python pipelines/pipeline.py

# Run pipeline + validate the results afterwards
run-and-validate:
	python pipelines/pipeline.py
	python -m tests.validate_pipeline --verbose

# Start the scheduled pipeline (runs every 30 minutes)
serve:
	python pipelines/pipeline.py --serve

# --- Individual Steps ---

# Run only the ingest step (no dbt)
ingest:
	python -m ingest.fetch_twitch
	python -m ingest.fetch_igdb
	python -m ingest.fetch_steam

# Run only dbt models
dbt-run:
	dbt run --project-dir game_pulse

# Run only dbt tests
dbt-test:
	dbt test --project-dir game_pulse

# Run dbt models + tests
dbt:
	dbt run --project-dir game_pulse
	dbt test --project-dir game_pulse

# --- Testing & Validation ---

# Validate pipeline output (checks schema, data quality, freshness)
validate:
	python -m tests.validate_pipeline --verbose

# Validate with relaxed freshness (useful if data is older)
validate-relaxed:
	python -m tests.validate_pipeline --verbose --max-hours 24

# --- Storage & Maintenance ---

# Preview what archival would do (no changes)
archive-dry-run:
	python -m maintenance.archive --dry-run

# Archive data older than 30 days to Parquet
archive:
	python -m maintenance.archive

# Archive with custom retention (e.g., keep last 7 days)
# Usage: make archive-days DAYS=7
archive-days:
	python -m maintenance.archive --days $(DAYS)

# --- Dashboard ---

# Launch the Streamlit dashboard
dashboard:
	streamlit run dashboard/app.py

# --- Start Everything ---

# Run pipeline once, then launch dashboard (one command to go from zero to running)
start:
	python pipelines/pipeline.py
	streamlit run dashboard/app.py

# Run pipeline once, validate, then launch dashboard
start-safe:
	python pipelines/pipeline.py
	python -m tests.validate_pipeline --verbose
	streamlit run dashboard/app.py

# --- Debugging ---

# Show recent pipeline logs
logs:
	tail -50 logs/pipeline.log

# Check what tables exist in the database
db-check:
	python -c "import duckdb; con = duckdb.connect('data/game_pulse.duckdb'); [print(r[0]) for r in con.execute('SHOW TABLES').fetchall()]; con.close()"

# Show row counts for all tables
db-counts:
	python -c "\
	import duckdb; \
	con = duckdb.connect('data/game_pulse.duckdb'); \
	tables = [r[0] for r in con.execute('SHOW TABLES').fetchall()]; \
	[print(f'{t}: {con.execute(f\"SELECT COUNT(*) FROM {t}\").fetchone()[0]} rows') for t in tables]; \
	con.close()"

.PHONY: run run-and-validate serve ingest dbt-run dbt-test dbt validate validate-relaxed archive-dry-run archive archive-days dashboard start start-safe logs db-check db-counts
