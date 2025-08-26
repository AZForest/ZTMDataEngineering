from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime
import os
import json
import random


# TODO: Use the @dag decorator to create a DAG that:
# * Runs every minute
# * Does not use catchup
@dag(
    "data_quality_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False
)
def data_quality_pipeline():

    CORRECT_PROB = 0.7

    def get_bookings_path(context):
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H-%M")
        return f"/tmp/data/bookings/{file_date}/bookings.json"

    def generate_booking_id(i):
        if random.random() < CORRECT_PROB:
            return i + 1

        return ""

    def generate_listing_id():
        if random.random() < CORRECT_PROB:
            return random.choice([1, 2, 3, 4, 5])

        return ""

    def generate_user_id(correct_prob=0.7):
        return random.randint(1000, 5000) if random.random() < correct_prob else ""

    def generate_booking_time(execution_date):
        if random.random() < CORRECT_PROB:
            return execution_date.strftime('%Y-%m-%d %H:%M:%S')

        return ""

    def generate_status():
        if random.random() < CORRECT_PROB:
            return random.choice(["confirmed", "pending", "cancelled"])

        return random.choice(["unknown", "", "error"])

    @task
    def generate_bookings():
        context = get_current_context()
        booking_path = get_bookings_path(context)

        num_bookings = random.randint(5, 15)
        bookings = []

        for i in range(num_bookings):
            booking = {
                "booking_id": generate_booking_id(i),
                "listing_id": generate_listing_id(),
                "user_id": generate_user_id(),
                "booking_time": generate_booking_time(context["execution_date"]),
                "status": generate_status()
            }
            bookings.append(booking)

        directory = os.path.dirname(booking_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(booking_path, "w") as f:
            json.dump(bookings, f, indent=4)

        print(f"Written to file: {booking_path}")

    # TODO: Create a data quality check task that reads bookings data and validates every record.
    # For every invalid record it should return a validation record that includes:
    # * A record position in an input file
    # * A list of identified violations
    #
    # Here is a list of validations it should perform:
    # * Check if each of the fields is missing
    # * Check if the "status" field has one of the valid values
    #
    # It should write all found anomalies into an input file.
    @task
    def data_quality_check():
        context = get_current_context()
        booking_path = get_bookings_path(context)

        with open(booking_path, "r") as f:
            records = json.load(f)

        result = []
        for i, record in enumerate(records):
            violations = []
            for field in record:
                if field == "status":
                    acceptable_vals = ["confirmed", "pending", "cancelled"]
                    if record[field] not in acceptable_vals:
                        violations.append(field)
                elif record[field] == "":
                    violations.append(field)
            if len(violations) > 0:
                result.append((i, violations))
        print(result)

        #read_path = f"/tmp/data/bookings/{file_date}/bookings.json"
        if len(result) > 0:
            write_date = booking_path[19:-14]
            error_path = f"/tmp/data/bookings/{write_date}/errors.json"
            with open(error_path, "w") as f:
                json.dump(result, f, indent=4)
            
            print(f"Written to file: {error_path}")

    # TODO: Define dependencies between tasks
    generate_bookings() >> data_quality_check()
# TODO: Create an instance of the DAG
demo_dag = data_quality_pipeline()