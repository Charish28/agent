import subprocess
import time

CLUSTER = "cluster-b59a"
REGION = "us-central1"
JOB_FILE = "gs://agentic-elt-dataproc/jobs/wordcount_job.py"

MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds


def check_cluster():
    print("Checking cluster status...")

    subprocess.run(
        [
            "gcloud", "dataproc", "clusters", "describe",
            CLUSTER,
            "--region", REGION
        ],
        check=True,
        stdout=subprocess.DEVNULL
    )

    print("Cluster is available.")


def submit_job():
    print("Submitting Spark job...")

    subprocess.run(
        [
            "gcloud", "dataproc", "jobs", "submit", "pyspark",
            JOB_FILE,
            f"--cluster={CLUSTER}",
            f"--region={REGION}"
        ],
        check=True
    )

    print("Job submitted successfully.")


def auto_retry():

    attempt = 1

    while attempt <= MAX_RETRIES:

        try:
            print(f"\nAttempt {attempt}/{MAX_RETRIES}")

            check_cluster()
            submit_job()

            print("Job completed successfully.")
            return  # Exit after success

        except subprocess.CalledProcessError as e:

            print(f"Error on attempt {attempt}")
            print("Command failed:", e.cmd)

            if attempt == MAX_RETRIES:
                print("Max retries reached. Job failed.")
                raise

            print(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

            attempt += 1


if __name__ == "__main__":
    auto_retry()

