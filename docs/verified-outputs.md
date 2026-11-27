# Verified outputs

Mọi khối dưới đây là output **thật**, chụp từ lần chạy ngày **2026-11-27** (copy nguyên văn, chỉ bỏ dòng log build/`Container …` của Compose; chỗ cắt ngắn ghi `…`).

| Mục | Giá trị |
|---|---|
| Image Kafka | `apache/kafka:4.3.1` (`kafka-topics.sh --version` -> `4.3.1`), digest `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| Docker | Engine 29.8.2 (Docker Engine - Community, linux/arm64), Compose 5.5.0 |
| Python client | `confluent-kafka==2.16.2` trên `python:3.12-slim` |
| Quy ước | Trong container `kafka`, CLI nằm ở `/opt/kafka/bin/kafka-*.sh`. Các khối ghi `kafka-topics.sh …` thực chất là `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh …`. Với cụm 3 broker thì `docker compose -f compose.cluster.yaml exec -T kafka-1 …`. |

Lưu ý khi đọc: UUID topic, consumer-id, IP container, `CreateTime`, tên `console-consumer-NNNNN` thay đổi mỗi lần chạy. Phân bổ key -> partition ổn định (cùng key + cùng số partition cho cùng partition) nhưng không nên coi số cụ thể là hợp đồng.

---

## 1. Single-node (`compose.yaml`, project `kafka-lab`)

Cổng host `127.0.0.1:9094`, topic `orders` do service `init` tạo (3 partition, RF 1).

```text
$ docker compose up -d --wait

$ docker compose ps
NAME                IMAGE                COMMAND                  SERVICE   CREATED         STATUS                   PORTS
kafka-lab-init-1    apache/kafka:4.3.1   "/__cacert_entrypoin…"   init      7 seconds ago   Up Less than a second    9092/tcp
kafka-lab-kafka-1   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka     7 seconds ago   Up 6 seconds (healthy)   9092/tcp, 127.0.0.1:9094->9094/tcp

$ docker compose logs init
Created topic orders.

$ docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --version
4.3.1

$ docker compose exec kafka which kafka-topics

$ docker compose exec kafka kafka-topics --version
OCI runtime exec failed: exec failed: unable to start container process: exec: "kafka-topics": executable file not found in $PATH

$ kafka-topics.sh --bootstrap-server localhost:9092 --list
orders

$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
Topic: orders	TopicId: EaAoPza8QHONEkGmcnkf3A	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 

$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 --replication-factor 1
Error while executing topic command : Topic 'orders' already exists.
[2026-11-27 02:27:48,984] ERROR org.apache.kafka.common.errors.TopicExistsException: Topic 'orders' already exists.
 (org.apache.kafka.tools.TopicCommand)

$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo --partitions 2 --replication-factor 1
Created topic demo.

$ kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic demo --partitions 1
Error while executing topic command : The topic demo currently has 2 partition(s); 1 would not be an increase.
[2026-11-27 02:27:51,073] ERROR org.apache.kafka.common.errors.InvalidPartitionsException: The topic demo currently has 2 partition(s); 1 would not be an increase.
 (org.apache.kafka.tools.TopicCommand)

$ kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic demo --partitions 4

$ kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic demo
Topic: demo	TopicId: iTlYPlzWSNqRm-W04BWL4w	PartitionCount: 4	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: demo	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: demo	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: demo	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: demo	Partition: 3	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 

$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic too-many-replicas --partitions 1 --replication-factor 3
Error while executing topic command : Unable to replicate the partition 3 time(s): The target replication factor of 3 cannot be reached because only 1 broker(s) are registered or some brokers have all their log directories cordoned.
[2026-11-27 02:27:54,054] ERROR org.apache.kafka.common.errors.InvalidReplicationFactorException: Unable to replicate the partition 3 time(s): The target replication factor of 3 cannot be reached because only 1 broker(s) are registered or some brokers have all their log directories cordoned.
 (org.apache.kafka.tools.TopicCommand)

$ kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic demo

$ docker compose run --rm producer
%4|1795746477.897|GETPID|orders-producer#producer-1| [thrd:main]: Failed to acquire idempotence PID from broker kafka:9092/1: Broker: Coordinator load in progress: retrying
delivered key=user-1 partition=2 offset=0
delivered key=user-1 partition=2 offset=1
delivered key=user-1 partition=2 offset=2
delivered key=user-3 partition=0 offset=0
delivered key=user-4 partition=0 offset=1
delivered key=user-3 partition=0 offset=2
delivered key=user-4 partition=0 offset=3
delivered key=user-3 partition=0 offset=4
delivered key=user-4 partition=0 offset=5
delivered key=user-2 partition=1 offset=0
delivered key=user-2 partition=1 offset=1
delivered key=user-2 partition=1 offset=2
done: sent=12 failed=0

$ kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
orders:0:6
orders:1:3
orders:2:3

$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 5000 --property print.key=true --property print.partition=true --property print.offset=true
Option --property is deprecated and will be removed in a future version. Use --formatter-property instead.
The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out. See https://kafka.apache.org/documentation/#consumer_rebalance_protocol
Partition:0	Offset:0	user-3	{"order_id": 3, "user": "user-3", "amount": 30}
Partition:0	Offset:1	user-4	{"order_id": 4, "user": "user-4", "amount": 40}
Partition:0	Offset:2	user-3	{"order_id": 7, "user": "user-3", "amount": 70}
Partition:0	Offset:3	user-4	{"order_id": 8, "user": "user-4", "amount": 80}
Partition:0	Offset:4	user-3	{"order_id": 11, "user": "user-3", "amount": 110}
Partition:0	Offset:5	user-4	{"order_id": 12, "user": "user-4", "amount": 120}
Partition:1	Offset:0	user-2	{"order_id": 2, "user": "user-2", "amount": 20}
Partition:1	Offset:1	user-2	{"order_id": 6, "user": "user-2", "amount": 60}
Partition:1	Offset:2	user-2	{"order_id": 10, "user": "user-2", "amount": 100}
Partition:2	Offset:0	user-1	{"order_id": 1, "user": "user-1", "amount": 10}
Partition:2	Offset:1	user-1	{"order_id": 5, "user": "user-1", "amount": 50}
Partition:2	Offset:2	user-1	{"order_id": 9, "user": "user-1", "amount": 90}
[2026-11-27 02:28:05,524] ERROR Error processing message, terminating consumer process:  (org.apache.kafka.tools.consumer.ConsoleConsumer)
org.apache.kafka.common.errors.TimeoutException
Processed a total of 12 messages

$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --partition 1 --offset 0 --max-messages 2 --property print.key=true
Option --property is deprecated and will be removed in a future version. Use --formatter-property instead.
user-2	{"order_id": 2, "user": "user-2", "amount": 20}
user-2	{"order_id": 6, "user": "user-2", "amount": 60}
Processed a total of 2 messages

$ docker compose run --rm -e IDLE_EXIT_SECONDS=5 consumer
consumer group=orders-consumer subscribed to orders
processed partition=2 offset=0 key=user-1 order_id=1 amount=10
processed partition=2 offset=1 key=user-1 order_id=5 amount=50
processed partition=2 offset=2 key=user-1 order_id=9 amount=90
processed partition=1 offset=0 key=user-2 order_id=2 amount=20
processed partition=1 offset=1 key=user-2 order_id=6 amount=60
processed partition=1 offset=2 key=user-2 order_id=10 amount=100
processed partition=0 offset=0 key=user-3 order_id=3 amount=30
processed partition=0 offset=1 key=user-4 order_id=4 amount=40
processed partition=0 offset=2 key=user-3 order_id=7 amount=70
processed partition=0 offset=3 key=user-4 order_id=8 amount=80
processed partition=0 offset=4 key=user-3 order_id=11 amount=110
processed partition=0 offset=5 key=user-4 order_id=12 amount=120
idle for 5s, exiting
closed, processed=12

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
console-consumer-89480
orders-consumer

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer   # consumer đã thoát

Consumer group 'orders-consumer' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
orders-consumer orders          0          6               6               0               -               -               -
orders-consumer orders          1          3               3               0               -               -               -
orders-consumer orders          2          3               3               0               -               -               -

$ docker compose run --rm -e MESSAGES=6 producer   # thêm 6 record khi chưa có consumer
delivered key=user-2 partition=1 offset=3
delivered key=user-2 partition=1 offset=4
delivered key=user-1 partition=2 offset=3
delivered key=user-1 partition=2 offset=4
delivered key=user-3 partition=0 offset=6
delivered key=user-4 partition=0 offset=7
done: sent=6 failed=0

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer   # có lag

Consumer group 'orders-consumer' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
orders-consumer orders          0          6               8               2               -               -               -
orders-consumer orders          1          3               5               2               -               -               -
orders-consumer orders          2          3               5               2               -               -               -

$ docker compose run -d --name orders-consumer-live consumer
3f2605598dff92c9944a8cd3263888634b5894b375e92fba03ee751f0a40d1a0

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer   # consumer đang chạy

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                          HOST            CLIENT-ID
orders-consumer orders          0          8               8               0               orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer
orders-consumer orders          1          5               5               0               orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer
orders-consumer orders          2          5               5               0               orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer --members

GROUP           CONSUMER-ID                                          HOST            CLIENT-ID       #PARTITIONS     
orders-consumer orders-consumer-5ff164e7-d9a6-4a15-9cbf-6e815ded570b /192.168.97.3   orders-consumer 3               

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer --state

GROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS
orders-consumer kafka:9092  (1)           range                Stable               1

$ docker logs orders-consumer-live
consumer group=orders-consumer subscribed to orders
processed partition=0 offset=6 key=user-3 order_id=3 amount=30
processed partition=0 offset=7 key=user-4 order_id=4 amount=40
processed partition=1 offset=3 key=user-2 order_id=2 amount=20
processed partition=1 offset=4 key=user-2 order_id=6 amount=60
processed partition=2 offset=3 key=user-1 order_id=1 amount=10
processed partition=2 offset=4 key=user-1 order_id=5 amount=50

$ docker stop orders-consumer-live   # gửi SIGTERM
orders-consumer-live

$ docker logs --tail 3 orders-consumer-live
processed partition=2 offset=3 key=user-1 order_id=1 amount=10
processed partition=2 offset=4 key=user-1 order_id=5 amount=50
closed, processed=6

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-consumer   # sau khi dừng

Consumer group 'orders-consumer' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
orders-consumer orders          0          8               8               0               -               -               -
orders-consumer orders          1          5               5               0               -               -               -
orders-consumer orders          2          5               5               0               -               -               -

$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group orders-consumer --topic orders --reset-offsets --to-earliest --dry-run

GROUP           TOPIC           PARTITION  NEW-OFFSET
orders-consumer orders          0          0
orders-consumer orders          1          0
orders-consumer orders          2          0

$ kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name orders --describe --all | grep -E 'retention.ms|segment.ms|min.insync|cleanup.policy'
  cleanup.policy=delete sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete}
  delete.retention.ms=86400000 sensitive=false synonyms={DEFAULT_CONFIG:log.cleaner.delete.retention.ms=86400000}
  local.retention.ms=-2 sensitive=false synonyms={DEFAULT_CONFIG:log.local.retention.ms=-2}
  min.insync.replicas=1 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:min.insync.replicas=1, DEFAULT_CONFIG:min.insync.replicas=1}
  retention.ms=604800000 sensitive=false synonyms={}
  segment.ms=604800000 sensitive=false synonyms={}

$ kafka-features.sh --bootstrap-server localhost:9092 describe
Feature: eligible.leader.replicas.version          SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 1                Epoch: 196
Feature: group.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 1                Epoch: 196
Feature: kraft.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 0                Epoch: 196
Feature: metadata.version                          SupportedMinVersion: 3.3-IV3          SupportedMaxVersion: 4.3-IV0          FinalizedVersionLevel: 4.3-IV0          Epoch: 196
Feature: share.version                             SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 1                Epoch: 196
Feature: streams.version                           SupportedMinVersion: 0                SupportedMaxVersion: 1                FinalizedVersionLevel: 1                Epoch: 196
Feature: transaction.version                       SupportedMinVersion: 0                SupportedMaxVersion: 2                FinalizedVersionLevel: 2                Epoch: 196

$ docker compose exec kafka ls /var/lib/kafka/data
__cluster_metadata-0
__consumer_offsets-0
…
bootstrap.checkpoint
cleaner-offset-checkpoint
demo-0.29b8c5afbec7408d9ae02b919a6998f1-delete
demo-1.7dd793b47dca4cd39de0e17fcaa2bdd3-delete
demo-2.7651e106509a40ef9b7bd97d87b911f7-delete
demo-3.f4fb31324f604af8bfd831647b341e3e-delete
log-start-offset-checkpoint
meta.properties
orders-0
orders-1
orders-2
recovery-point-offset-checkpoint
replication-offset-checkpoint

$ docker compose exec kafka kafka-dump-log (orders-2)
Dumping /var/lib/kafka/data/orders-2/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 2 count: 3 baseSequence: 0 lastSequence: 2 producerId: 0 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 0 CreateTime: 1795746477382 size: 241 magic: 2 compresscodec: none crc: 1215354950 isvalid: true
| offset: 0 CreateTime: 1795746477382 keySize: 6 valueSize: 47 sequence: 0 headerKeys: [] key: user-1 payload: {"order_id": 1, "user": "user-1", "amount": 10}
| offset: 1 CreateTime: 1795746477382 keySize: 6 valueSize: 47 sequence: 1 headerKeys: [] key: user-1 payload: {"order_id": 5, "user": "user-1", "amount": 50}
| offset: 2 CreateTime: 1795746477382 keySize: 6 valueSize: 47 sequence: 2 headerKeys: [] key: user-1 payload: {"order_id": 9, "user": "user-1", "amount": 90}
baseOffset: 3 lastOffset: 4 count: 2 baseSequence: 0 lastSequence: 1 producerId: 1 producerEpoch: 0 partitionLeaderEpoch: 0 isTransactional: false isControl: false deleteHorizonMs: OptionalLong.empty position: 241 CreateTime: 1795746498994 size: 181 magic: 2 compresscodec: none crc: 1789590285 isvalid: true
| offset: 3 CreateTime: 1795746498994 keySize: 6 valueSize: 47 sequence: 0 headerKeys: [] key: user-1 payload: {"order_id": 1, "user": "user-1", "amount": 10}
| offset: 4 CreateTime: 1795746498994 keySize: 6 valueSize: 47 sequence: 1 headerKeys: [] key: user-1 payload: {"order_id": 5, "user": "user-1", "amount": 50}
```

### 1.0 Trạng thái init

Dòng `kafka-lab-init-1 … Up Less than a second` ở `docker compose ps` đầu tiên chỉ là ảnh chụp ngay lúc `init` đang chạy. Vài giây sau (lệnh `docker compose ps -a`):

```text
NAME                IMAGE                COMMAND                  SERVICE   CREATED              STATUS                        PORTS
kafka-lab-init-1    apache/kafka:4.3.1   "/__cacert_entrypoin…"   init      About a minute ago   Exited (0) 26 seconds ago     
kafka-lab-kafka-1   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka     About a minute ago   Up About a minute (healthy)   9092/tcp, 127.0.0.1:9094->9094/tcp
```

### 1.1 Producer chạy từ máy host qua listener EXTERNAL

Chạy bằng venv trên host (`pip install -c app/constraints.txt confluent-kafka`), nối `localhost:9094`:

```text
$ BOOTSTRAP_SERVERS=localhost:9094 MESSAGES=4 python app/producer.py
delivered key=user-2 partition=1 offset=5
delivered key=user-1 partition=2 offset=5
delivered key=user-3 partition=0 offset=8
delivered key=user-4 partition=0 offset=9
done: sent=4 failed=0
$ kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
orders:0:10
orders:1:6
orders:2:6
```

### 1.2 Cạm bẫy: đặt một biến `KAFKA_*` thì image bỏ cấu hình mặc định

```text
$ docker run --rm -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 apache/kafka:4.3.1
CLUSTER_ID not set. Setting it to default value: "5L6g3nShT-eMCtK--X86sw"
===> Configuring ...
===> Launching ...
===> Using provided cluster id 5L6g3nShT-eMCtK--X86sw ...
Exception in thread "main" org.apache.kafka.common.config.ConfigException: Missing required configuration "process.roles" which has no default value. at org.apache.kafka.common.config.ConfigDef.parseValue(ConfigDef.java:537) at … (stack trace trimmed)
```

### 1.3 Ghi chú về các lệnh trên

- Lần chạy producer đầu tiên sau khi broker mới khởi động có thể in một dòng cảnh báo `GETPID … Coordinator load in progress: retrying` (idempotent producer xin PID khi coordinator còn đang load). Producer tự retry và vẫn gửi đủ; không phải lỗi.
- `console-consumer --timeout-ms` luôn kết thúc bằng `ERROR … TimeoutException` rồi `Processed a total of N messages`: đó là cách nó thoát khi hết dữ liệu.
- Sau khi `--delete --topic demo`, thư mục `demo-N.<hash>-delete` còn nằm trên đĩa một lúc (broker xóa bất đồng bộ).

---

## 2. Cụm 3 broker (`compose.cluster.yaml`, project `kafka-cluster`)

3 node KRaft combined (`kafka-1..3`), listener INTERNAL `kafka-N:19092` cho inter-broker + container, EXTERNAL `127.0.0.1:19094/19095/19096`, topic `orders` 3 partition, RF 3, `min.insync.replicas=2`. Phần này chạy `--describe` mỗi 30 giây trong 150 giây để xác nhận ISR không tụt (khác hẳn cấu hình quảng bá `localhost` trong bài cũ).

```text
$ docker compose -f compose.cluster.yaml ps -a
NAME                      IMAGE                COMMAND                  SERVICE   CREATED          STATUS                      PORTS
kafka-cluster-init-1      apache/kafka:4.3.1   "/__cacert_entrypoin…"   init      20 seconds ago   Exited (0) 11 seconds ago   
kafka-cluster-kafka-1-1   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka-1   20 seconds ago   Up 19 seconds (healthy)     9092/tcp, 127.0.0.1:19094->9094/tcp
kafka-cluster-kafka-2-1   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka-2   20 seconds ago   Up 19 seconds (healthy)     9092/tcp, 127.0.0.1:19095->9094/tcp
kafka-cluster-kafka-3-1   apache/kafka:4.3.1   "/__cacert_entrypoin…"   kafka-3   20 seconds ago   Up 19 seconds (healthy)     9092/tcp, 127.0.0.1:19096->9094/tcp

$ docker compose -f compose.cluster.yaml logs init
Created topic orders.

$ kafka-metadata-quorum.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 describe --status
ClusterId:              MkU3OEVBNTcwNTJENDM2Qk
LeaderId:               3
LeaderEpoch:            1
HighWatermark:          61
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   233
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://kafka-1:9093"]}, {"id": 2, "endpoints": ["CONTROLLER://kafka-2:9093"]}, {"id": 3, "endpoints": ["CONTROLLER://kafka-3:9093"]}]
CurrentObservers:       []

$ kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --describe --topic orders   # t=0
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 

$ docker compose -f compose.cluster.yaml run --rm producer

%4|1795746583.818|GETPID|orders-producer#producer-1| [thrd:main]: Failed to acquire idempotence PID from broker kafka-3:19092/3: Broker: Coordinator load in progress: retrying
delivered key=user-1 partition=2 offset=0
delivered key=user-1 partition=2 offset=1
delivered key=user-1 partition=2 offset=2
delivered key=user-3 partition=0 offset=0
delivered key=user-4 partition=0 offset=1
delivered key=user-3 partition=0 offset=2
delivered key=user-4 partition=0 offset=3
delivered key=user-3 partition=0 offset=4
delivered key=user-4 partition=0 offset=5
delivered key=user-2 partition=1 offset=0
delivered key=user-2 partition=1 offset=1
delivered key=user-2 partition=1 offset=2
done: sent=12 failed=0

$ docker compose -f compose.cluster.yaml run --rm -e IDLE_EXIT_SECONDS=5 consumer

consumer group=orders-consumer subscribed to orders
processed partition=1 offset=0 key=user-2 order_id=2 amount=20
processed partition=1 offset=1 key=user-2 order_id=6 amount=60
processed partition=1 offset=2 key=user-2 order_id=10 amount=100
processed partition=0 offset=0 key=user-3 order_id=3 amount=30
processed partition=0 offset=1 key=user-4 order_id=4 amount=40
processed partition=0 offset=2 key=user-3 order_id=7 amount=70
processed partition=0 offset=3 key=user-4 order_id=8 amount=80
processed partition=0 offset=4 key=user-3 order_id=11 amount=110
processed partition=0 offset=5 key=user-4 order_id=12 amount=120
processed partition=2 offset=0 key=user-1 order_id=1 amount=10
processed partition=2 offset=1 key=user-1 order_id=5 amount=50
processed partition=2 offset=2 key=user-1 order_id=9 amount=90
idle for 5s, exiting
closed, processed=12

# --- chờ 150 giây, mỗi 30 giây in --describe (chỉ dòng Isr) ---

$ [t=+30s] kafka-topics.sh --describe --topic orders
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 

$ [t=+60s] kafka-topics.sh --describe --topic orders
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 

$ [t=+90s] kafka-topics.sh --describe --topic orders
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 

$ [t=+120s] kafka-topics.sh --describe --topic orders
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 

$ [t=+150s] kafka-topics.sh --describe --topic orders
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2	Elr: 	LastKnownElr: 

$ kafka-topics.sh --describe --under-replicated-partitions   # (không in gì = không có partition thiếu replica)

$ kafka-console-producer.sh --producer-property acks=all (1 dòng)
Warning: --producer-property is deprecated and will be removed in a future version. Use --command-property instead.

$ kafka-get-offsets.sh --topic orders
orders:0:7
orders:1:3
orders:2:3
```

### 2.1 Dừng broker

Dùng `docker compose run --rm --no-deps producer`: nếu thiếu `--no-deps`, Compose sẽ khởi động lại broker đang dừng (vì `producer` phụ thuộc `init` -> 3 broker) và làm sai thí nghiệm.

```text
$ kafka-topics.sh --describe --topic orders   # trước khi dừng
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 3	Replicas: 2,3,1	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1,2,3	Elr: 	LastKnownElr: 

$ docker compose -f compose.cluster.yaml stop kafka-3

$ kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092 --describe --topic orders   # kafka-3 đã dừng
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 1,2	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1,2	Elr: 	LastKnownElr: 

$ kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092 --describe --under-replicated-partitions --topic orders
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 1,2	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1,2	Elr: 	LastKnownElr: 

$ docker compose -f compose.cluster.yaml run --rm --no-deps -e MESSAGES=6 producer   # ISR=2 >= min.insync.replicas=2
delivered key=user-1 partition=2 offset=7
delivered key=user-1 partition=2 offset=8
delivered key=user-3 partition=0 offset=9
delivered key=user-4 partition=0 offset=10
delivered key=user-2 partition=1 offset=7
delivered key=user-2 partition=1 offset=8
done: sent=6 failed=0

$ docker compose -f compose.cluster.yaml stop kafka-2

$ kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic orders   # chỉ còn kafka-1
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1	Elr: 2	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 1	Replicas: 2,3,1	Isr: 1	Elr: 2	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1	Elr: 2	LastKnownElr: 

$ docker compose -f compose.cluster.yaml run --rm --no-deps -e MESSAGES=2 -e MESSAGE_TIMEOUT_MS=15000 producer   # ISR=1 < 2
%3|1795746929.095|FAIL|orders-producer#producer-1| [thrd:kafka-3:19092/bootstrap]: kafka-3:19092/bootstrap: Failed to resolve 'kafka-3:19092': Name or service not known (after 2ms in state CONNECT)
FAILED key=user-2 error=KafkaError{code=_MSG_TIMED_OUT,val=-192,str="Local: Message timed out"}
FAILED key=user-1 error=KafkaError{code=_MSG_TIMED_OUT,val=-192,str="Local: Message timed out"}
done: sent=2 failed=2

$ kafka-get-offsets.sh --bootstrap-server kafka-1:19092 --topic orders
Skip getting offsets for topic-partition orders-1 due to error: org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment. Call: listOffsets(api=LIST_OFFSETS)
orders:0:11
orders:2:9

$ docker compose -f compose.cluster.yaml start kafka-2 kafka-3

$ kafka-topics.sh --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 --describe --topic orders   # sau khi start lại kafka-2, kafka-3
Topic: orders	TopicId: 1N-o3IDrQkip8__M0ubhEg	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 1	Leader: 1	Replicas: 2,3,1	Isr: 1,2,3	Elr: 	LastKnownElr: 
	Topic: orders	Partition: 2	Leader: 1	Replicas: 3,1,2	Isr: 1,2,3	Elr: 	LastKnownElr: 

$ docker compose -f compose.cluster.yaml run --rm --no-deps -e MESSAGES=2 producer
delivered key=user-2 partition=1 offset=9
delivered key=user-1 partition=2 offset=9
done: sent=2 failed=0

$ kafka-get-offsets.sh --topic orders
orders:0:11
orders:1:10
orders:2:10
```

Điểm cần nhớ từ kết quả:
- Dừng 1 broker (ISR=2 >= `min.insync.replicas=2`): producer `acks=all` vẫn thành công.
- Dừng tiếp broker thứ hai (ISR=1 < 2): producer `acks=all` không ghi được (`_MSG_TIMED_OUT`, ép `MESSAGE_TIMEOUT_MS=15000` cho nhanh; mặc định chờ lâu hơn nhiều, không đo). Cột `Elr: 2` xuất hiện trong `--describe` (Eligible Leader Replicas, Kafka 4.x).
- Hai record bị timeout đó **không** được ghi: end offset trước khi lỗi là `orders:0:11`, `orders:2:9` (partition 1 không đọc được vì lúc đó cụm chỉ còn 1 broker: `Skip getting offsets … orders-1`); sau khi cụm phục hồi và gửi thêm 2 record, end offset là 11/10/10.
- Start lại 2 broker: ISR trở về 1,2,3 khi kiểm tra sau 30 giây và producer thành công.
