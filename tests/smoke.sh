#!/usr/bin/env bash
# End-to-end smoke test cho lab Kafka (single-node).
# Chạy từ bất kỳ đâu: bash tests/smoke.sh   (cần Docker + Compose; build, chạy, rồi xóa sạch)
set -euo pipefail
cd "$(dirname "$0")/.."

# Cô lập: project Compose riêng + cổng riêng, không đụng stack `kafka-lab` (cổng 9094) của người học.
export COMPOSE_PROJECT_NAME=kafka-lab-smoke
export KAFKA_EXTERNAL_PORT="${SMOKE_KAFKA_PORT:-19194}"
export GROUP_ID=smoke-group
export IDLE_EXIT_SECONDS=5
N=12                       # số message producer gửi
BIN=/opt/kafka/bin

cleanup() { docker compose --profile app down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; exit 1; }
contains() { # mô tả, chuỗi mong đợi, thực tế
  case "$3" in *"$2"*) pass "$1" ;; *) fail "$1 (expected '$2' in: $3)" ;; esac
}
equals() { [ "$3" = "$2" ] && pass "$1" || fail "$1 (expected '$2', got '$3')"; }
kafka() { docker compose exec -T kafka "$@"; }

cleanup  # bỏ dư lượng của lần chạy trước (nếu có)
docker compose --profile app build --quiet
docker compose up -d --wait kafka
pass "broker healthy"

# --- topic: service init tạo, rồi kiểm tra bằng CLI ---
docker compose run --rm init >/dev/null 2>&1 || fail "init service (kafka-topics --create --if-not-exists)"
desc=$(kafka $BIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders)
contains "topic orders có 3 partition" "PartitionCount: 3" "$desc"
contains "topic orders có RF 1" "ReplicationFactor: 1" "$desc"

# --- producer: N message, 0 lỗi ---
out=$(docker compose run --rm -e MESSAGES=$N producer 2>&1)
contains "producer báo gửi đủ" "done: sent=$N failed=0" "$out"
delivered=$(printf '%s\n' "$out" | grep -c '^delivered ' || true)
equals "delivery callback chạy cho mọi record" "$N" "$delivered"

# --- cùng key luôn cùng partition ---
for u in user-1 user-2 user-3 user-4; do
  parts=$(printf '%s\n' "$out" | grep "key=$u " | sed 's/.*partition=\([0-9]*\).*/\1/' | sort -u | wc -l | tr -d ' ')
  equals "key $u chỉ vào 1 partition" 1 "$parts"
done

# --- tổng end offset = N ---
total=$(kafka $BIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders | awk -F: '{s+=$3} END{print s}')
equals "tổng log-end-offset = $N" "$N" "$total"

# --- consumer group: đọc hết, commit sau xử lý ---
cout=$(docker compose run --rm consumer 2>&1)
contains "consumer xử lý đủ rồi đóng êm" "closed, processed=$N" "$cout"

# --- lag và current-offset qua kafka-consumer-groups.sh ---
grp=$(kafka $BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group smoke-group 2>/dev/null)
cur=$(printf '%s\n' "$grp" | awk '$1=="smoke-group" && $2=="orders" {s+=$4} END{print s+0}')
lag=$(printf '%s\n' "$grp" | awk '$1=="smoke-group" && $2=="orders" {s+=$6} END{print s+0}')
equals "tổng CURRENT-OFFSET = $N" "$N" "$cur"
equals "tổng LAG = 0" 0 "$lag"

# --- gửi thêm 5 message, lag phải = 5 khi chưa có consumer ---
docker compose run --rm -e MESSAGES=5 producer >/dev/null 2>&1
lag=$(kafka $BIN/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group smoke-group 2>/dev/null \
  | awk '$1=="smoke-group" && $2=="orders" {s+=$6} END{print s+0}')
equals "sau khi gửi thêm 5, LAG = 5" 5 "$lag"

# --- consumer chạy lại chỉ đọc phần còn thiếu (offset đã commit được tôn trọng) ---
cout=$(docker compose run --rm consumer 2>&1)
contains "consumer chạy lại chỉ xử lý 5 record mới" "closed, processed=5" "$cout"

# --- listener EXTERNAL từ máy host: kiểm tra cổng mở và broker quảng bá localhost:<cổng> ---
adv=$(kafka $BIN/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-name 1 --describe --all \
  | grep -m1 'advertised.listeners=' || true)
contains "EXTERNAL quảng bá localhost:$KAFKA_EXTERNAL_PORT" "EXTERNAL://localhost:$KAFKA_EXTERNAL_PORT" "$adv"
(exec 3<>"/dev/tcp/127.0.0.1/$KAFKA_EXTERNAL_PORT") 2>/dev/null && pass "cổng host 127.0.0.1:$KAFKA_EXTERNAL_PORT mở" \
  || fail "cổng host 127.0.0.1:$KAFKA_EXTERNAL_PORT không mở"

echo "ALL PASS"
