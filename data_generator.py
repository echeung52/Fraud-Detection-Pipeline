import random
from faker import Faker
from datetime import datetime
import pandas as pd
from confluent_kafka import Producer
import json
import time  # Import time for delay

# Initialize Faker
fake = Faker()

# Kafka Configuration
producer_conf = {'bootstrap.servers': 'localhost:9092'}  # Kafka broker address
producer = Producer(producer_conf)

# Categories provided in the dataset
categories = [
    'misc_net', 'grocery_pos', 'entertainment', 'gas_transport', 'misc_pos',
    'grocery_net', 'shopping_net', 'shopping_pos', 'food_dining', 'personal_care',
    'health_fitness', 'travel', 'kids_pets', 'home'
]

def generate_transaction():
    """Generate a random transaction."""
    transaction = {
        'Unnamed: 0': random.randint(0, 1000000),  # Random unique ID
        'trans_date_trans_time': fake.date_time_between(start_date='-1y', end_date='now').strftime('%Y-%m-%d %H:%M:%S'),
        'cc_num': fake.credit_card_number(),
        'merchant': fake.company(),
        'category': random.choice(categories),
        'amt': round(random.uniform(1.0, 5000.0), 2),  # Transaction amount
        'first': fake.first_name(),
        'last': fake.last_name(),
        'gender': random.choice(['M', 'F']),
        'street': fake.street_address(),
        'city': fake.city(),
        'state': fake.state_abbr(),
        'zip': fake.zipcode(),
        'lat': round(float(fake.latitude()), 6),
        'long': round(float(fake.longitude()), 6),
        'city_pop': random.randint(1000, 1000000),
        'job': fake.job(),
        'dob': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
        'trans_num': fake.uuid4(),
        'unix_time': int(datetime.now().timestamp()),
        'merch_lat': round(float(fake.latitude()), 6),
        'merch_long': round(float(fake.longitude()), 6),
    }
    return transaction

def delivery_report(err, msg):
    """Callback for Kafka delivery reports."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

if __name__ == "__main__":
    while True:
        # Generate a single transaction
        transaction = generate_transaction()
        
        # Convert to JSON for Kafka
        transaction_json = json.dumps(transaction)

        # Produce to Kafka topic
        producer.produce(
            'raw-transactions', 
            key=str(transaction['Unnamed: 0']), 
            value=transaction_json,
            callback=delivery_report
        )
        
        # Flush producer buffer to ensure delivery
        producer.poll(1)

        # Wait for 5 seconds before sending the next transaction
        time.sleep(5)
