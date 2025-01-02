from confluent_kafka import Consumer, Producer, KafkaError
import pickle
import json
import pandas as pd
from datetime import datetime

# Load pre-trained encoders and scaler
category_encoder = pickle.load(open('category_encoder.pkl', 'rb'))
cc_num_encoder = pickle.load(open('cc_num_encoder.pkl', 'rb'))
scaler = pickle.load(open('scaler.pkl', 'rb'))

# Kafka Configuration for Consumer and Producer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'preprocessor-group',
    'auto.offset.reset': 'earliest'
}
producer_conf = {'bootstrap.servers': 'localhost:9092'}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Subscribe to the raw-transactions topic
consumer.subscribe(['raw-transactions'])

def preprocess(transaction):
    """Preprocess a transaction."""
    # Drop unused columns
    unused_cols = ['Unnamed: 0', 'first', 'last', 'unix_time', 'street', 'gender',
                   'job', 'dob', 'city', 'state', 'trans_num', 'merchant']
    for col in unused_cols:
        transaction.pop(col, None)

    # Feature engineering for date and time
    transaction['trans_date_trans_time'] = pd.to_datetime(transaction['trans_date_trans_time'])
    transaction['trans_day'] = transaction['trans_date_trans_time'].day
    transaction['trans_month'] = transaction['trans_date_trans_time'].month
    transaction['trans_year'] = transaction['trans_date_trans_time'].year
    transaction['trans_hour'] = transaction['trans_date_trans_time'].hour
    transaction['trans_minute'] = transaction['trans_date_trans_time'].minute
    transaction.pop('trans_date_trans_time', None)

    # Encode 'category' using the category encoder
    if transaction['category'] in category_encoder.classes_:
        transaction['category'] = int(category_encoder.transform([transaction['category']])[0])
    else:
        transaction['category'] = -1  # Default value for unseen categories

    # Encode 'cc_num' using the cc_num encoder
    if transaction['cc_num'] in cc_num_encoder.classes_:
        transaction['cc_num'] = int(cc_num_encoder.transform([transaction['cc_num']])[0])
    else:
        transaction['cc_num'] = -1  # Default value for unseen credit card numbers

    # Scale numerical columns
    for col in ['amt', 'zip', 'city_pop']:
        transaction[col] = float(scaler.transform([[transaction[col]]])[0][0])

    return transaction

def delivery_report(err, msg):
    """Callback for Kafka delivery reports."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

if __name__ == "__main__":
    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # End of partition event
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Deserialize the raw transaction
            transaction = json.loads(msg.value().decode('utf-8'))
            print(transaction)

            # Preprocess the transaction
            processed_transaction = preprocess(transaction)

            # Produce the preprocessed transaction to the processed-transactions topic
            producer.produce(
                'processed-transactions',
                key=str(processed_transaction['trans_day']),  # Changed to ensure a unique key
                value=json.dumps(processed_transaction),
                callback=delivery_report
            )
            producer.poll(1)  # Ensure message delivery

    except KeyboardInterrupt:
        print("Preprocessing stopped.")
    finally:
        consumer.close()
