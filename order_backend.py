import json 
import time
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 20

producer = KafkaProducer(bootstrap_servers=['localhost:29092'], api_version=(0,11,5))

print("Going to be generating order after 2s")
print("Will generate one unique order every 2s")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i * 2,
        "items": "burger, sandwich"
    }

    producer.send(
            ORDER_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
    )
    print(f"Done sending...{i}")
    time.sleep(2)