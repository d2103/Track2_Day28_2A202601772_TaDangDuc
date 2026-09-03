# Optional extension — Kaggle T4 + vLLM

Extension này dành cho học viên đã hoàn thành core. Kaggle GPU chỉ giải quyết tài
nguyên inference LLM; nó không tự giải quyết Kafka, Docker, state persistence,
network tunnel, quota, reproducibility hoặc failure recovery.

## Khi nào nên dùng

- Muốn thử OpenAI-compatible LLM serving với vLLM.
- Kaggle đang cấp T4 và session còn quota.
- Core tests/readiness đã pass ở local hoặc browser workspace.

Không dùng P100 làm baseline. [Kaggle thông báo P100 nghỉ ngày
2026-09-15](https://www.kaggle.com/product-announcements/735239) và T4x2 vẫn được
duy trì; availability/quota vẫn có thể thay đổi theo tài khoản.

## Notebook cells gợi ý

Kiểm tra GPU trước:

```bash
!nvidia-smi
!pip install -q "vllm==0.26.0"
```

Chạy model nhỏ phù hợp T4:

```bash
!vllm serve Qwen/Qwen3-4B-Instruct-2507 \
  --host 0.0.0.0 --port 8000 \
  --dtype half --max-model-len 4096 \
  --gpu-memory-utilization 0.85
```

Lệnh bám theo [`vllm serve` 0.26.0](https://docs.vllm.ai/en/v0.26.0/cli/serve/)
và [model card Qwen3-4B-Instruct-2507](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507).

Kiểm tra endpoint trong cùng session:

```bash
!curl -s http://127.0.0.1:8000/v1/models
```

## Đưa endpoint ra ngoài Kaggle

Notebook không có IP công khai, nên cần một tunnel. Cloudflare quick tunnel
không cần tài khoản:

```bash
!wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O /usr/local/bin/cloudflared
!chmod +x /usr/local/bin/cloudflared
!cloudflared tunnel --url http://localhost:8000 --no-autoupdate
```

Lệnh này chặn cell (không tự kết thúc); URL in ra dạng
`https://<random>.trycloudflare.com`. Trước khi dùng URL đó, tự kiểm tra đủ ba
tín hiệu mà gate `gpu` yêu cầu — thiếu một là trượt:

```bash
u="https://<random>.trycloudflare.com"
!curl -s {u}/health -o /dev/null -w "health=%{{http_code}}
"
!curl -s {u}/version; echo
!curl -s {u}/metrics | grep -c "^vllm:"
```

`ports.template` và `compose.yaml` mặc định đọc vLLM ở
`http://host.docker.internal:8001/v1`, và Prometheus scrape cùng địa chỉ đó
(`monitoring/prometheus.yml`, job `lab28-vllm-optional`). Trỏ thẳng
`LAB28_VLLM_BASE_URL` vào URL tunnel làm API gọi được, nhưng để Prometheus
scrape một cổng 8001 chết — tunnel đổi hostname mỗi phiên nên không sửa được
static config. Chạy cầu nối một chiều trên máy host để mọi thứ vẫn nói chuyện
qua cổng 8001 như thiết kế:

```bash
uv run python scripts/vllm_local_forward.py https://<random>.trycloudflare.com
```

Giữ lệnh đó chạy nền, giữ nguyên `LAB28_VLLM_BASE_URL` ở giá trị mặc định
(không override), rồi build lại container API.

Session Kaggle ngắt sau khoảng 20 phút không tương tác; tunnel chết theo và
phải lấy URL mới. Không commit URL tunnel hay token vào Git.

## Bài tập Operator

Viết một adapter thay CPU classifier nhưng vẫn trả contract có output, model
identifier/version, latency và trace ID. So sánh P50/P95, memory và failure mode.
Không hard-code URL tunnel hay token vào notebook/repository.

## Giới hạn cần ghi trong ADR

- Session và GPU quota có thể hết giữa buổi.
- Tunnel public tạo thêm rủi ro security và latency.
- Model download làm cold start lâu; cần cache/preflight.
- Hai T4 không tự động tăng tốc nếu không cấu hình tensor parallel phù hợp.
- Kết quả extension không phải bằng chứng Kafka/Delta/MLflow core đã hoạt động.
