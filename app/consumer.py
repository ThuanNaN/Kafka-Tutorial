"""Consumer trong consumer group: xử lý xong rồi mới commit offset; thoát êm khi nhận SIGINT/SIGTERM."""
import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = os.environ.get("TOPIC", "orders")
GROUP_ID = os.environ.get("GROUP_ID", "orders-consumer")
# 0 = chạy mãi cho tới khi Ctrl+C; >0 = thoát sau ngần ấy giây không có record mới (dùng cho test)
IDLE_EXIT_SECONDS = float(os.environ.get("IDLE_EXIT_SECONDS", "0"))

running = True


def stop(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "client.id": "orders-consumer",
        "auto.offset.reset": "earliest",  # group mới chưa có offset thì đọc từ đầu
        "enable.auto.commit": False,  # tự commit sau khi xử lý xong
    }
)
consumer.subscribe([TOPIC])
print(f"consumer group={GROUP_ID} subscribed to {TOPIC}")

processed = 0
last_msg = time.monotonic()
try:
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            if IDLE_EXIT_SECONDS and time.monotonic() - last_msg > IDLE_EXIT_SECONDS:
                print(f"idle for {IDLE_EXIT_SECONDS:g}s, exiting")
                break
            continue
        if msg.error():
            raise KafkaException(msg.error())
        last_msg = time.monotonic()
        order = json.loads(msg.value())
        print(
            f"processed partition={msg.partition()} offset={msg.offset()} "
            f"key={msg.key().decode()} order_id={order['order_id']} amount={order['amount']}"
        )
        processed += 1
        # Chỉ commit SAU khi xử lý xong: crash giữa chừng thì record được đọc lại (at-least-once).
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()  # rời group sạch sẽ, commit/rebalance gọn
    print(f"closed, processed={processed}")
