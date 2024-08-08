import time

import schedule


def job1():
    print("Job 1 is running")


def job2():
    print("Job 2 is running")


# Schedule the jobs
schedule.every(10).seconds.do(job1)
schedule.every(20).seconds.do(job2)


# Function to get the list of scheduled jobs
def list_scheduled_jobs():
    jobs = schedule.jobs
    for job in jobs:
        print(f"Job: {job.job_func.__name__}, Interval: {job.interval} {job.unit}, Next run: {job.next_run}")


# Run the scheduled jobs
while True:
    schedule.run_pending()
    list_scheduled_jobs()
    time.sleep(1)
