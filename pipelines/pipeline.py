from prefect import flow, task
import subprocess

@task
def run_ingest():
    subprocess.run(["python", "ingest/fetch.py"], check=True)

@task
def run_dbt():
    subprocess.run(["dbt", "run"], cwd="game_pulse", check=True)

@flow(name="game-pulse")
def pipeline():
    run_ingest()
    run_dbt()

if __name__ == "__main__":
    pipeline()