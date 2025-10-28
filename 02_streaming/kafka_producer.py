from kafka import KafkaProducer
import json, random, time

producer = KafkaProducer(bootstrap_servers="kafka:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

while True:
    data = {
        "device_id": random.randint(1, 5),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2)
    }
    producer.send("sensor_data", value=data)
    print(f"Sent: {data}")
    time.sleep(1)
