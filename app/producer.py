"""Producer: gửi đơn hàng vào topic, key = user để cùng user luôn vào cùng partition."""
import json
import os
import sys

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.environ.get("TOPIC", "orders")
MESSAGES = int(os.environ.get("MESSAGES", "12"))
# Tùy chọn: rút ngắn thời gian chờ giao record (ms). Mặc định để librdkafka quyết định.
MESSAGE_TIMEOUT_MS = os.environ.get("MESSAGE_TIMEOUT_MS")
USERS = ["user-1", "user-2", "user-3", "user-4"]

failed = 0


def on_delivery(err, msg):
    """Chạy khi broker xác nhận (hoặc từ chối) từng record."""
    global failed
    if err is not None:
        failed += 1
        print(f"FAILED key={msg.key().decode()} error={err}", file=sys.stderr)
        return
    print(f"delivered key={msg.key().decode()} partition={msg.partition()} offset={msg.offset()}")


config = {
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "client.id": "orders-producer",
}
if MESSAGE_TIMEOUT_MS:
    config["message.timeout.ms"] = int(MESSAGE_TIMEOUT_MS)
producer = Producer(config)

for i in range(MESSAGES):
    user = USERS[i % len(USERS)]
    value = json.dumps({"order_id": i + 1, "user": user, "amount": 10 * (i + 1)})
    producer.produce(TOPIC, key=user, value=value, on_delivery=on_delivery)
    producer.poll(0)  # phục vụ callback đã sẵn sàng

producer.flush(float(MESSAGE_TIMEOUT_MS or 30000) / 1000 + 5)  # chờ mọi record được xác nhận
print(f"done: sent={MESSAGES} failed={failed}")
sys.exit(1 if failed else 0)
