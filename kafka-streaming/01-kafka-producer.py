import json
import random
import textwrap
import time
from datetime import datetime

from confluent_kafka import Producer

def generate_order():
    countries = [
        "USA",
        "Canada",
        "UK",
        "Germany",
        "France",
        "Australia",
        "Japan",
        "Ireland",
    ]
    order = {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 10),
        "total_price": round(random.uniform(20.0, 1000.0), 2),
        "customer_country": random.choice(countries),
        "merchant_country": random.choice(countries),
        "order_date": datetime.now().isoformat(),
    }
    return order

def main():
    
    config = {
        "bootstrap.servers": "localhost:9092"
    }

    producer = Producer(config)

    topic = "orders"

    def delivery_callback(err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            print(
                textwrap.dedent(
                f"""
                    Produced event to topic {msg.topic()}:
                    key = {msg.key().decode('utf-8')}
                    value = {msg.value().decode('utf-8')}
                """)
            )

    while True:
        order = generate_order()
        print(f"Sending order: {order}")

        producer.produce(
            topic,
            key=str(order["customer_id"]),
            value=json.dumps(order),
            callback=delivery_callback
        )

        producer.poll(0)

        time.sleep(1)

if __name__ == "__main__":
    main()