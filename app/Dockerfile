# --- Stage 1: cài dependency vào prefix riêng ---
FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt constraints.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt -c constraints.txt

# --- Stage 2: runtime, không có build tool, chạy bằng user thường ---
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN useradd --create-home --uid 10001 appuser
WORKDIR /app
COPY --from=builder /install /usr/local
COPY producer.py consumer.py ./
USER appuser
CMD ["python", "producer.py"]
