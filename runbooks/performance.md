# Performance profile

Chạy `uv run python load-tests/run_profile.py --requests 200 --workers 8`, rồi
lặp với 16 workers. Ghi P50/P95/P99, API CPU/RAM, vLLM queue/tokens, Kafka lag và
error rate. `/ready` là baseline; nhóm phải đo thêm `/api/v1/ask` với corpus đại diện:

```text
uv run python load-tests/run_profile.py --profile ask --requests 100 --workers 4
```

Đo thêm một lượt trực tiếp vào API (`--url http://localhost:8000`) để tách phần
do rate limit của gateway ra khỏi phần do chính ứng dụng. Đọc
`successful_latency_ms` chứ không chỉ `latency_ms`: request bị 429 trả về gần
như tức thì và sẽ kéo percentile tổng xuống một cách sai lệch.

Không suy ra production capacity từ laptop. Luôn ghi hardware, model, dataset,
concurrency, warm-up và degraded policy.
