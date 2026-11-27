# Kafka Tutorial - VLAI

Dự án thực hành của khóa **Apache Kafka** (VLAI): một broker Kafka 4.3.1 chạy KRaft bằng Docker Compose,
cộng với producer và consumer Python (`confluent-kafka`) cho topic `orders`. Có thêm cụm 3 broker để xem replication và ISR.

Khóa học: https://vlai.aivietnam.edu.vn/tutorials/apache-kafka

```
                        mạng Docker `kafka-net`
 ┌──────────┐  kafka:9092 (INTERNAL)   ┌───────────────────────────────┐
 │ producer │ ───────────────────────▶ │  kafka  (apache/kafka:4.3.1)  │
 ├──────────┤                          │  KRaft: broker + controller   │
 │ consumer │ ◀─────────────────────── │  topic orders: 3 partition    │
 │ (group)  │                          │  controller :9093 (nội bộ)    │
 └──────────┘                          └───────────────┬───────────────┘
                                                       │ EXTERNAL :9094
 Ứng dụng trên máy host  ── 127.0.0.1:9094 ────────────┘  (chỉ bind 127.0.0.1)
```

## Yêu cầu

- Docker Engine 29.x và Docker Compose v5.x (đã kiểm chứng: Engine 29.8.2, Compose 5.5.0, linux/arm64)
- Không cần cài Java hay Python trên máy: mọi thứ chạy trong container.
- Dự án **độc lập**: chỉ cần clone repo này.

## Chạy bài lab (single-node)

```bash
git clone https://github.com/ThuanNaN/Kafka-Tutorial.git
cd Kafka-Tutorial

docker compose up -d --wait    # chạy broker, chờ healthy, service `init` tạo topic orders rồi thoát
docker compose ps -a           # kafka (healthy); init Exited (0)
```

Gửi và nhận record bằng ứng dụng Python:

```bash
docker compose run --rm producer    # gửi 12 record, mỗi record in partition/offset
docker compose run --rm consumer    # đọc trong consumer group `orders-consumer`; Ctrl+C để dừng
```

Output thật của producer (lần đầu sau khi broker khởi động có thể có thêm một dòng cảnh báo `GETPID … Coordinator load in progress: retrying`, producer tự retry):

```text
delivered key=user-1 partition=2 offset=0
delivered key=user-1 partition=2 offset=1
delivered key=user-1 partition=2 offset=2
delivered key=user-3 partition=0 offset=0
delivered key=user-4 partition=0 offset=1
…
done: sent=12 failed=0
```

Cùng một key luôn vào cùng một partition (`user-1` -> partition 2, `user-2` -> partition 1; `user-3` và `user-4` cùng rơi vào partition 0).
Consumer in `processed partition=… offset=… key=…` cho từng record, commit offset **sau khi** xử lý xong,
và khi nhận Ctrl+C/SIGTERM thì đóng êm (`closed, processed=N`).

Tùy chỉnh: `MESSAGES=30 docker compose run --rm producer`, `GROUP_ID=my-group docker compose run --rm consumer`,
`IDLE_EXIT_SECONDS=5` để consumer tự thoát sau 5 giây không có record mới.

### Dùng từ máy host

Listener EXTERNAL mở ở `127.0.0.1:9094`:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -c app/constraints.txt confluent-kafka
BOOTSTRAP_SERVERS=localhost:9094 MESSAGES=4 python app/producer.py
```

Cổng 9094 bị chiếm? Copy `.env.example` thành `.env` và đổi `KAFKA_EXTERNAL_PORT`. Một biến này đổi cả cổng publish lẫn
địa chỉ broker quảng bá (hai giá trị phải trùng nhau, nếu không client trên host nhận về địa chỉ sai).

## Lệnh Kafka CLI trong container

Image `apache/kafka` chỉ có script **có đuôi `.sh`** trong `/opt/kafka/bin/`; không có lệnh `kafka-topics` (không đuôi, đó là image Confluent).
Chạy `docker compose exec kafka kafka-topics --version` sẽ báo `executable file not found in $PATH`.

```bash
K="docker compose exec -T kafka /opt/kafka/bin"

$K/kafka-topics.sh --version
$K/kafka-topics.sh --bootstrap-server localhost:9092 --list
$K/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
$K/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
$K/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 5000 \
  --property print.key=true --property print.partition=true --property print.offset=true
$K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
$K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer
```

Output thật của `--describe --topic orders` (UUID topic mỗi lần một khác):

```text
Topic: orders	TopicId: EaAoPza8QHONEkGmcnkf3A	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
```

`kafka-get-offsets.sh` sau lần producer đầu (12 record):

```text
orders:0:6
orders:1:3
orders:2:3
```

`kafka-consumer-groups.sh --describe` khi consumer **đang chạy** (mở một terminal chạy `docker compose run --rm consumer`):

```text
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                          HOST            CLIENT-ID
orders-consumer orders          0          8               8               0               orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer
orders-consumer orders          1          5               5               0               orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer
orders-consumer orders          2          5               5               0               orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer
```

Khi consumer đã dừng, các cột CONSUMER-ID/HOST/CLIENT-ID là `-` và có dòng `Consumer group 'orders-consumer' has no active members.`
Gửi thêm record lúc chưa có consumer thì cột LAG tăng (đã kiểm chứng: gửi thêm 6 record, LAG mỗi partition = 2).
Mọi output chi tiết khác (alter, lỗi RF quá lớn, `--members`, `--state`, `--reset-offsets --dry-run`, dump log…) nằm trong
[`docs/verified-outputs.md`](docs/verified-outputs.md).

## Cụm 3 broker (replication, ISR)

```bash
docker compose -f compose.cluster.yaml up -d --wait        # project `kafka-cluster`, tách biệt với stack single-node
docker compose -f compose.cluster.yaml run --rm producer   # acks=all, min.insync.replicas=2

C="docker compose -f compose.cluster.yaml exec -T kafka-1 /opt/kafka/bin"
$C/kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --describe --topic orders
```

```text
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 
```

ISR giữ nguyên 3 phần tử trong suốt 150 giây theo dõi (xem `docs/verified-outputs.md`). Dừng một broker:

```bash
docker compose -f compose.cluster.yaml stop kafka-3
```

```text
$ kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092 --describe --topic orders
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 1,2	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1,2	Elr: 	LastKnownElr: 
```

ISR còn 2 (>= `min.insync.replicas=2`) nên producer `acks=all` vẫn ghi được. Dừng thêm `kafka-2` (ISR=1 < 2) thì producer không ghi được:

```text
FAILED key=user-2 error=KafkaError{code=_MSG_TIMED_OUT,val=-192,str="Local: Message timed out"}
FAILED key=user-1 error=KafkaError{code=_MSG_TIMED_OUT,val=-192,str="Local: Message timed out"}
done: sent=2 failed=2
```

(Chạy với `MESSAGE_TIMEOUT_MS=15000` cho nhanh.) Start lại broker (`docker compose -f compose.cluster.yaml start kafka-2 kafka-3`) thì ISR về lại 1,2,3.

Cổng host của cụm: `127.0.0.1:19094`, `19095`, `19096` (đổi bằng `KAFKA_EXTERNAL_PORT_1/2/3`).
Dọn dẹp: `docker compose -f compose.cluster.yaml down -v`.

## Bảng cổng và listener

| Listener | Trong container | Quảng bá cho client là | Ai dùng |
|---|---|---|---|
| INTERNAL (single-node) | `:9092` | `kafka:9092` | container trong `kafka-net` (producer, consumer, CLI) |
| EXTERNAL (single-node) | `:9094` | `localhost:9094` | ứng dụng trên máy host, publish `127.0.0.1:9094` |
| CONTROLLER | `:9093` | `kafka:9093` (chỉ trong quorum) | KRaft, không dành cho client |
| INTERNAL (cụm) | `:19092` | `kafka-N:19092` | inter-broker (nhân bản) + container |
| EXTERNAL (cụm) | `:9094` | `localhost:19094/19095/19096` | host, publish `127.0.0.1:<cổng>` cho kafka-1/2/3 |
| CONTROLLER (cụm) | `:9093` | `kafka-N:9093` | quorum 3 node |
| Smoke test | `:9094` | `localhost:19194` | `tests/smoke.sh` (project `kafka-lab-smoke`) |

## Dừng và dọn dẹp

```bash
docker compose --profile app down       # dừng, xóa container + network, GIỮ dữ liệu (volume kafka-data)
docker compose --profile app down -v    # xóa luôn volume: mất toàn bộ topic và record
```

## Kiểm tra tự động

```bash
bash tests/smoke.sh    # build, tạo topic, produce 12 record, consume qua group, kiểm tra offset/lag, rồi dọn sạch
```

Chạy trong project Compose riêng (`kafka-lab-smoke`, cổng 19194, đổi bằng `SMOKE_KAFKA_PORT`) nên không đụng stack bạn đang chạy.
Kết thúc bằng `ALL PASS`.

## Cấu trúc

| Đường dẫn | Vai trò |
|---|---|
| `compose.yaml` | Single-node KRaft, 2 listener, healthcheck, `init` tạo topic, `producer`/`consumer` (profile `app`) |
| `compose.cluster.yaml` | Cụm 3 broker, listener INTERNAL riêng cho inter-broker, `min.insync.replicas=2` |
| `app/producer.py` | Key theo user, `acks=all`, `enable.idempotence=true`, delivery callback in partition/offset |
| `app/consumer.py` | Consumer group, commit thủ công sau khi xử lý, thoát êm khi SIGINT/SIGTERM |
| `app/Dockerfile`, `app/constraints.txt` | Build multi-stage, chạy user thường; khóa phiên bản `confluent-kafka` |
| `tests/smoke.sh` | Kiểm tra end-to-end |
| `docs/verified-outputs.md` | Mọi lệnh và output thật (nguồn đối chiếu cho bài học) |
| `.env.example` | Mẫu cấu hình cổng (`.env` không được commit) |

## Lưu ý

- Image `apache/kafka` chỉ dùng cấu hình mặc định khi **không có** biến `KAFKA_*` nào. Chỉ đặt `KAFKA_ADVERTISED_LISTENERS`
  là broker không lên: `Missing required configuration "process.roles"`. Vì vậy compose khai báo đủ bộ.
- Cluster nhiều broker mà chỉ quảng bá `localhost:<cổng>` (không có listener INTERNAL riêng) là cấu hình hỏng:
  các broker nhân bản qua địa chỉ quảng bá nên mỗi broker nối nhầm vào chính nó và ISR tụt dần về 1.
- `docker compose run producer` (không `--no-deps`) sẽ khởi động lại các broker đã dừng vì `producer` phụ thuộc `init`.
  Khi thử dừng broker trong cụm, dùng `run --rm --no-deps`.
- Broker không có xác thực (PLAINTEXT): chỉ bind `127.0.0.1`. Muốn mở ra mạng, hãy thêm TLS/SASL trước.
- Dữ liệu Kafka nằm trong volume Docker (`kafka-data`, `kafka-N-data`). `down -v` là xóa hết.
- `CLUSTER_ID` của cụm là giá trị cố định trong `compose.cluster.yaml` (đổi bằng biến `CLUSTER_ID`, chuỗi 22 ký tự từ `kafka-storage.sh random-uuid`).

## Cấu trúc khóa học

Xem toàn bộ bài học tại https://vlai.aivietnam.edu.vn/tutorials/apache-kafka.
