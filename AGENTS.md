# AGENTS.md

Lab Apache Kafka (KRaft, `apache/kafka:4.3.1`, Python `confluent-kafka`) đi kèm khóa Apache Kafka của VLAI
(https://vlai.aivietnam.edu.vn/tutorials/apache-kafka). Đây là **phiên bản chạy được hoàn chỉnh**, dùng làm nguồn đối chiếu
cho nội dung bài học: lệnh, output và giá trị cấu hình trong bài phải khớp `docs/verified-outputs.md`.

## Chạy

```bash
docker compose up -d --wait            # broker + tạo topic orders (3 partition, RF 1), cổng host 127.0.0.1:9094
docker compose run --rm producer       # gửi 12 record (key = user-1..4)
docker compose run --rm consumer       # consumer group, Ctrl+C để dừng êm
docker compose -f compose.cluster.yaml up -d --wait   # cụm 3 broker (project riêng, cổng 19094-19096)
bash tests/smoke.sh                    # end-to-end, project kafka-lab-smoke, cổng 19194, tự dọn
```

## Quy ước

- Image Kafka **ghim đúng tag** (`4.3.1`) ở cả hai file compose. Đổi tag thì chạy lại `tests/smoke.sh`, cụm 3 broker và cập nhật `docs/verified-outputs.md` + README.
- Image `apache/kafka`: CLI chỉ có dạng `/opt/kafka/bin/kafka-*.sh` (không có `kafka-topics` không đuôi). Đặt bất kỳ biến `KAFKA_*` nào là image bỏ cấu hình mặc định, nên compose khai báo đủ bộ.
- Hai listener bắt buộc: INTERNAL (container, quảng bá tên service) và EXTERNAL (host, `127.0.0.1`, quảng bá `localhost:<cổng publish>`). Cụm 3 broker cần thêm listener INTERNAL riêng cho inter-broker; quảng bá `localhost` thì ISR tụt về 1.
- Dependency Python khóa trong `app/constraints.txt`; Dockerfile multi-stage, chạy user thường.
- Output đưa vào README/`docs/verified-outputs.md` phải chụp từ lần chạy thật, không viết tay.
- Thí nghiệm dừng broker trong cụm: dùng `run --rm --no-deps`, nếu không Compose khởi động lại broker đang dừng.
