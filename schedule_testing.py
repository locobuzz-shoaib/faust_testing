import json
import time
from datetime import datetime, timedelta

import schedule


# Define the job functions
def job1():
    print("Job 1 is running")


def job2():
    print("Job 2 is running")


def one_time_job():
    print("This job runs only once")


# Function to schedule a job with a name
def schedule_job_with_name(job_func, interval, unit, job_name):
    job = getattr(schedule.every(interval), unit).do(job_func)
    job.job_name = job_name
    return job


# Function to schedule a one-time job with a name
def schedule_one_time_job(run_time, job_name):
    job = schedule.every().day.at(run_time).do(one_time_job)

    def job_wrapper():
        one_time_job()
        schedule.cancel_job(job)

    job.job_func = job_wrapper
    job.job_name = job_name


# Schedule jobs with names
schedule_job_with_name(job1, 10, 'seconds', 'Recurring Job 1')
schedule_job_with_name(job2, 20, 'seconds', 'Recurring Job 2')

# Calculate the time to run the job
run_time = (datetime.now() + timedelta(seconds=30)).strftime("%H:%M:%S")
schedule_one_time_job(run_time, 'One-Time Job')


# Function to get the list of scheduled jobs as JSON
def list_scheduled_jobs():
    jobs = schedule.jobs
    job_list = []
    for job in jobs:
        job_info = {
            "job_name": getattr(job, 'job_name', 'Unnamed Job'),
            "job_func": job.job_func.__name__,
            "interval": job.interval,
            "unit": job.unit,
            "next_run": job.next_run.strftime("%Y-%m-%d %H:%M:%S")
        }
        job_list.append(job_info)
    return json.dumps(job_list, indent=4)


# Example usage
if __name__ == "__main__":
    # Run the scheduled jobs
    while True:
        schedule.run_pending()
        print(list_scheduled_jobs())
        time.sleep(10)
