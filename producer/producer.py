import json
import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:29092',   # ✅ 使用 IPv4 回环地址
    max_block_ms=5000,
    request_timeout_ms=3000,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = [
    "click",
    "like",
    "comment",
    "share"
]

while True:

    data = {
        "user_id": random.randint(1, 1000),
        "video_id": random.randint(1, 100),
        "action": random.choice(actions),
        "timestamp": int(time.time())
    }

    producer.send("user-behavior", data)

    print(data)

    time.sleep(1)