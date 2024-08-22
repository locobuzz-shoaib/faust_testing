import time
import redis
import json
import uuid

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Function to add a task with JSON data
def add_task_with_json(task_name, timestamp, data):
    task_id = str(uuid.uuid4())  # TODO: Make the
    task_key = f"{task_name}:{task_id}"  # Create a unique key for the task

    # Serialize JSON data and store it in Redis (as a string)
    redis_client.set(task_key, json.dumps(data))

    # Schedule the task with its timestamp
    redis_client.zadd('schedule', {task_key: timestamp})
    print(f"Task '{task_key}' scheduled at {time.ctime(timestamp)} with data: {data}")

# Function to calculate the next execution time based on custom logic
def calculate_next_time(current_time):
    # Custom logic to calculate the next time (e.g., add 60 seconds)
    return current_time + 60

# Function to get due tasks and their associated JSON data
def get_due_tasks_with_json(current_time):
    due_tasks = redis_client.zrangebyscore('schedule', 0, current_time)
    task_data = []
    for task_key in due_tasks:
        data = json.loads(redis_client.get(task_key).decode('utf-8'))
        task_data.append((task_key.decode('utf-8'), data))
    return task_data

# Function to process tasks and potentially reschedule them
def process_tasks_with_reschedule(tasks):
    for task_key, data in tasks:
        print(f"Processing task: {task_key} with data: {data}")

        # Calculate the next time this task should be run
        next_time = calculate_next_time(time.time())

        # Reschedule the task if needed
        if next_time:
            print(f"Rescheduling task '{task_key}' to {time.ctime(next_time)}")
            add_task_with_json(task_key.split(":")[0], next_time, data)

        # Remove the current task from the schedule
        redis_client.zrem('schedule', task_key)
        redis_client.delete(task_key)  # Clean up the JSON data

# Example JSON data for two tasks at the same time
json_data_1 = {"email": "user_a@example.com", "message": "Hello User A!"}
json_data_2 = {"email": "user_b@example.com", "message": "Hello User B!"}

# Adding tasks with the same timestamp (current time + 60 seconds)
current_time = time.time()
scheduled_time = current_time + 60  # 1 minute from now

add_task_with_json("Send Email to User A", scheduled_time, json_data_1)  # Task 1
add_task_with_json("Send Email to User B", scheduled_time, json_data_2)  # Task 2

# Simulate checking for due tasks periodically
while True:
    current_time = time.time()
    due_tasks = get_due_tasks_with_json(current_time)

    if due_tasks:
        process_tasks_with_reschedule(due_tasks)
    else:
        print("No tasks to process at this time.")

    # Wait for a short period before checking again
    time.sleep(30)
