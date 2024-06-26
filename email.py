import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers=['localhost:29092'],
    api_version=(0,11,5)
) 

email_sent_so_far = set()
print("Email is listening")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]
        print(f"Sending email to {customer_email}")

        email_sent_so_far.add(customer_email)
        print(f"So far email sent to {len(email_sent_so_far)} unique emails.")

        