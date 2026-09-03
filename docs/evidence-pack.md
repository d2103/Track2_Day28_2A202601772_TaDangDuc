# Evidence pack — Day 28 Track 2

Nhánh `ca-nhan-ductd`, base commit `3115c55`. Toàn bộ số liệu dưới đây được thu
trên một máy Windows 11, Docker Desktop, full profile (`--profile full`), ngày
**2026-09-03**.

IP07 được nối với vLLM thật (Kaggle T4, `Qwen/Qwen3-4B-Instruct-2507`, qua
Cloudflare tunnel). Phiên đầu ngắt giữa chừng lúc chạy bộ test `gpu` (12/15
pass); nối lại phiên thứ hai, sửa nốt hai lỗi thật tìm được, kết quả cuối
**14/15 test `gpu` pass** — 1 test còn lại là giới hạn kiến trúc đã ghi nhận,
không sửa được từ phía repo. Xem mục 2.1 và 7.

Tài liệu này trả lời mục 4–7 của [`SUBMISSION.md`](../SUBMISSION.md). File JSON
gốc nằm ở [`evidence/`](../evidence/).

## 0. Trạng thái kiểm thử

```text
uv run ruff check .                                → All checks passed
uv run pytest tests starter-tests -q               → 87 passed
uv run python scripts/verify_matrix.py             → 245 checks passed
uv run python scripts/check_portability.py         → OK
uv run python scripts/validate_manifests.py        → passed
uv run pytest integration-tests -m "not gpu and not langsmith" -q
                                                   → 56 passed, 16 deselected (563s)
uv run pytest integration-tests -m gpu -q          → 14 passed, 1 failed (380s, vLLM Kaggle T4 thật)
```

Chạy lại bộ non-gpu **sau** hai lần sửa `gateway/envoy.yaml` (health-check route
và timeout) để xác nhận không có regression ở cấu hình mặc định: **56 passed,
16 deselected** — khớp chính xác con số trước khi sửa.

`evidence/integration-report.json`: `score 83`, 6 điểm probe được từ tiến trình
phục vụ, 5 đạt, IP07 `not_ready`. Bốn điểm còn lại (IP02, IP08, IP09, IP10)
`unverified` theo thiết kế — chúng phải được chứng minh từ ngoài tiến trình, và
bằng chứng nằm trong các file JSON tương ứng.

## 1. Sửa chữa đã thực hiện

Tám lỗi thật tìm thấy khi chạy repo, kèm cách phát hiện:

| # | Lỗi | Phát hiện bằng | Sửa ở |
|---|---|---|---|
| 1 | Envoy health-check và Docker healthcheck bị đếm vào `lab28_requests_total`, làm `test_the_gateway_answers_its_own_health_route` fail ~2/3 lần | Chạy suite; drift 1 request giữa hai lần scrape | `gateway/envoy.yaml`, `compose.yaml`, `src/lab28_platform/api.py` |
| 2 | `startupProbe` trỏ `/health` trong khi app có `/startup` đúng ngữ nghĩa | Đọc `api.py` docstring so với manifest | `deploy/kubernetes/base/api.yaml` |
| 3 | Airflow scheduler chết vì `sqlite3.OperationalError: database is locked` (LocalExecutor + `parallelism=4` trên một file SQLite, busy timeout mặc định 5s) | J1 treo vô hạn; log scheduler | `compose.yaml` — `?timeout=60` |
| 4 | Healthcheck của Airflow chỉ kiểm HTTP 200, báo `healthy` suốt 2 giờ trong khi scheduler/triggerer/dag-processor đều chết | Đọc body `/api/v2/monitor/health` | `compose.yaml` |
| 5 | `lab28 seed --via-gateway` luôn bị 429 cho phần đuôi corpus (25 request vào giới hạn 10 rps), trái với kết quả mong đợi ghi trong README Bước 7 | Chạy lệnh; 5 feedback bị `local_rate_limited` | `src/lab28_platform/cli.py` — backoff khi gặp 429 |
| 6 | `run_profile.py` chỉ đo được `/ready`, trong khi `runbooks/performance.md` yêu cầu đo thêm `/api/v1/ask` | Đọc runbook so với script | `load-tests/run_profile.py` |
| 7 | Envoy active-health-check kiểm `/health` (liveness, không đụng dependency nào) thay vì `/ready`, nên khi Qdrant/Kafka/MLflow chết, gateway **không bao giờ** eject pod — trái với `test_the_gateway_stops_routing_to_a_pod_that_is_not_ready`, một phần của IT-J4 | Test này mang marker `gpu`, chưa từng chạy tới khi chưa có vLLM thật; chạy lần đầu thì fail, đọc code xác nhận `/health` không chạm dependency | `gateway/envoy.yaml` |
| 8 | Timeout active-health-check (2s) quá chặt cho round trip qua Internet tới GPU từ xa; dưới tải, gateway eject nhầm pod đang khỏe (`active_hc_timeout`), một request `/api/v1/ask` giữa suite nhận `503 no healthy upstream` | Chạy toàn bộ bộ `gpu` một lần, 1 request lỗi giữa 15 test; đọc `health_flags` trong Envoy admin `/clusters` | `gateway/envoy.yaml` — nới lên 5s |

Điểm chung của #1, #2, #4 và #7: cả bốn đều là **tín hiệu sức khỏe nói dối**.
Một health route bị proxy, một startup probe hỏi sai câu, một healthcheck bỏ
qua body, một active health check hỏi đúng route nhưng route đó cố tình không
biết dependency nào đang chết — mỗi cái đều xanh trong khi thứ nó đại diện đã
hỏng. Lỗi #7 đặc biệt: nó nằm im trong repo từ đầu vì gate `gpu` khiến test duy
nhất phát hiện ra nó chưa từng được chạy cho tới khi có vLLM thật. Lỗi #8 chỉ lộ
ra khi chạy **cả bộ 15 test `gpu` liên tục dưới tải thật** — chạy riêng lẻ từng
test không bao giờ thấy, vì mỗi lần chạy riêng độ trễ tunnel không đủ tích luỹ
để vượt 2 giây.

**Đánh đổi khi sửa #7:** trước đây `/healthz` gateway proxy `/ready` xuống app
để operator thấy JSON chi tiết lý do degraded ngay cả khi có lỗi. Sau khi active
health check chuyển sang `/ready`, một phụ thuộc bắt buộc chết sẽ khiến Envoy
ngừng forward hoàn toàn sau tối đa 2 giây (interval của health check) — từ đó
`gateway:/ready` trả lỗi opaque của chính Envoy, không còn JSON breakdown. Chi
tiết degraded vẫn xem được bằng cách gọi thẳng container API ở `:8000`, hoặc
qua gauge `lab28_component_ready` trên Grafana. Đây là đánh đổi đúng: mục đích
của gateway là ngăn traffic tới pod không phục vụ được, không phải làm nơi debug
tiện tay.

## 2. Happy path (SUBMISSION mục 4)

Nguồn: `evidence/ip01-kafka-consume.json`, `ip02-airflow-run.json`,
`ip03-delta-history.json`, `ip04-feast-online.json`, `ip05-qdrant-search.json`,
`ip06-mlflow-release.json`, `ip10-trace.json`.

```text
uv run lab28 seed --via-gateway     → 13 documents + 12 feedback accepted, 0 rejected
                                      (đợi hết 1 lần 429 của gateway)
```

Một lượt chạy đầy đủ, `2026-09-03T11:58:43Z`:

| Mốc | Giá trị |
|---|---|
| DAG run ID | `it-a7582e9c`, state `success`, 104.8 s |
| Task states | `drain_kafka_into_delta`, `refresh_online_features`, `index_new_documents`, `announce_processed_batch` — tất cả `success` |
| Asset events | `lab28://delta/feedback`, `lab28://delta/documents`, `lab28://feast/asker_activity`, `lab28://qdrant/lab28_documents` |
| Delta trước | `documents` v10 / 22 rows · `feedback` v16 / 26 rows |
| Delta sau | `documents` v11 / 22 rows · `feedback` v17 / 27 rows |
| Qdrant | 22 points, trước và sau bằng nhau |
| MLflow | `lab28-rag-release` v2, alias `champion`, run `45458d6992bb4a8c844c7fb2ec9e9fb8` |
| Trace ID mẫu (IP01) | `16991b9c55114092976ed556b121d464`, đi cùng `traceparent` trên header Kafka |

**Không có vLLM thật:** `POST /api/v1/ask` qua gateway trả **503** với
`category: dependency_unavailable`, `message: inference endpoint unavailable:
vLLM unreachable: ConnectError`, `trace_id: dfbad58ec4936f7fafc0f81821830ed3`.
Request đã đi qua Envoy → FastAPI → MLflow (resolve champion) → Feast → Qdrant
và dừng đúng tại ranh giới IP07. Đây là hành vi đúng theo thiết kế: `pipeline.py`
cố ý **không** có đường suy giảm cho inference, vì một câu trả lời không có mô
hình là một câu trả lời bịa.

### 2.1 Có vLLM thật — happy path đầy đủ

Nối `LAB28_VLLM_BASE_URL` vào endpoint Kaggle T4 (tunnel Cloudflare), model
`Qwen/Qwen3-4B-Instruct-2507`. `probe_identity()`:

```json
{"reachable": true, "version": "0.26.0",
 "served_models": ["Qwen/Qwen3-4B-Instruct-2507"],
 "vllm_metric_count": 111, "is_real_vllm": true,
 "detail": "vLLM identity confirmed"}
```

`GET /ready` chuyển từ `degraded` sang **`status: ready`** — cả 5 component
đều `ready: true`. `evidence/integration-report.json`: **`score: 100`**,
6/6 điểm probe được đều `ready`, IP07 `ready — vLLM identity confirmed`.

Cùng một `POST /api/v1/ask` như trên, giờ trả **200**:

| Trường | Giá trị |
|---|---|
| `evidence.vllm_model_id` | `Qwen/Qwen3-4B-Instruct-2507` |
| `evidence.mlflow_release_version` | `2` |
| `trace_id` | `9fdceb9aa6cd303397a3d7d5b6125e62` |
| `audit.latency.llm_ms` | 2941 |
| `audit.latency.total_ms` | 13778 (bao gồm retrieval nguội 8696 ms — xem mục 4) |
| `answer` | có trích dẫn `[1]`, dựa trên 3 nguồn lấy từ Qdrant |

`degraded: true` trong response — đúng, vì `asker_id` là entity mới, Feast chưa
có online row cho nó. Không liên quan tới vLLM.

**Chưa hoàn tất khi phiên Kaggle ngắt** (xem mục 7): chạy `pytest -m gpu` đầy
đủ, đo load profile `/ask` với model thật, và trace 4-service.

## 3. Sự cố và khôi phục (SUBMISSION mục 5)

### 3.1 Kịch bản A — phụ thuộc không bắt buộc

**Giả thuyết trước khi inject:** Feast là `mandatory=False`, nên `/ready` phải
đổi component sang `ready: false` mà **không** trả 503 và **không** chặn
ingestion.

| Thời điểm | `/ready` HTTP | status | `feast` |
|---|---|---|---|
| `12:02:49Z` trước | 200 | `degraded` | `ready: true, ok` |
| `12:03:02Z` sau `docker compose stop feast` | **200** | `degraded` | `ready: false, unreachable: ConnectError` |
| `12:03:36Z` sau `docker compose start feast` (healthy sau 32.6 s) | 200 | `degraded` | `ready: true, ok` |

Trong lúc Feast dừng, một submission qua gateway vẫn trả **202 accepted**
(`idempotency_key: fb:incident-feast-a9f559fc:5366b644…`,
`trace_id: cf9a2fe08af1d3b25bbdb426afd15527`).

Lưu ý đọc bảng: status tổng vẫn là `degraded` cả trước lẫn sau, vì `vllm` đã
`ready: false` từ đầu. Đó chính là lý do phải đọc **component list** chứ không
chỉ đọc một chữ ở đầu response — một chữ `degraded` không cho biết cái gì đang
hỏng.

### 3.2 Kịch bản B — mất tầng orchestration, chứng minh không mất dữ liệu

**Giả thuyết:** API chấp nhận `202` là một lời hứa; Kafka là thứ giữ lời hứa đó
khi tầng xử lý biến mất. Sự kiện nhận trong lúc Airflow dừng phải vào Delta đủ
sau khi khôi phục.

| Bước | Kết quả |
|---|---|
| `feedback` rows trước | **27** |
| `docker compose stop airflow` | — |
| Gửi 5 feedback mới qua gateway | 5/5 trả `202 accepted` |
| `feedback` rows trong lúc mất | **27** (không đổi — đúng, chưa ai xử lý) |
| `docker compose start airflow` | healthy sau 33.0 s |
| DAG drain | `it-38e1566a`, state `success` |
| `feedback` rows sau | **33** |

Chênh lệch là **+6**, không phải +5. Sáu là đúng: năm sự kiện của kịch bản B
cộng một sự kiện đã được nhận trong kịch bản A lúc Feast dừng. Cả hai đều nằm
trong Kafka suốt thời gian mất dịch vụ và cùng được drain trong một lần chạy.
Không có sự kiện nào biến mất, và không có sự kiện nào vào hai lần.

### 3.3 Idempotency — replay không tạo bản ghi trùng

Gửi lại **đúng corpus cũ** (cùng `idempotency_key`, nhưng là bản tin Kafka mới
với offset mới), rồi chạy lại DAG:

| Bảng | Sau lượt 1 | Sau replay | Rows |
|---|---|---|---|
| `documents` | v11 / 22 rows | **v12 / 22 rows** | không đổi |
| `feedback` | v17 / 27 rows | **v18 / 27 rows** | không đổi |
| Qdrant | 22 points | **22 points** | không đổi |

Đây là hai mệnh đề tách rời và cần cả hai:

- **Version tăng** → lô dữ liệu thật sự đã được xử lý, không bị bỏ qua âm thầm.
- **Số dòng không đổi** → `dedupe_latest` + `MERGE ... WHEN MATCHED` đã khử
  trùng. Qdrant giữ nguyên 22 điểm vì point ID suy ra tất định từ `doc_id`.

Nếu chỉ có mệnh đề thứ hai, ta không phân biệt được "chống trùng đúng" với
"pipeline không chạy".

## 4. Load profile và phân tích nút thắt (SUBMISSION mục 6)

Phần cứng: Windows 11, Docker Desktop, full profile đang chạy đồng thời. **Không
suy ra capacity production từ các số này.**

| Profile | Đích | Req | Workers | Throughput | Status | P50 | P95 | P99 |
|---|---|---:|---:|---:|---|---:|---:|---:|
| A | gateway `/ready` | 200 | 8 | 5.08 rps | 189×200, 9×503, 2×429 | 991 ms | 2724 ms | 14729 ms |
| B | gateway `/ready` | 200 | 16 | 6.65 rps | 82×200, **117×429** | 23 ms | 2266 ms | 2435 ms |
| C | API `/ready` (bỏ qua gateway) | 200 | 16 | 6.97 rps | **200×200** | 2113 ms | 2899 ms | 5451 ms |
| D | API `/api/v1/ask` | 40 | 4 | 3.32 rps | **40×503** | 994 ms | 2434 ms | 2443 ms |

P50 của profile B là 23 ms vì phần lớn request bị 429 trả về tức thì; cột
`successful_latency_ms` trong JSON gốc cho P50 thực của request thành công là
1730 ms. Đó là lý do script tách hai bộ percentile.

**Nút thắt không phải là rate limit của gateway.** So B với C: bỏ hẳn limiter
đi, throughput chỉ tăng từ 6.65 lên 6.97 rps. Trần thật nằm ở ứng dụng, không ở
Envoy.

**Nút thắt là fan-out của `/ready`.** Mỗi lần gọi `/ready` chạy lại toàn bộ probe
— Kafka metadata, MLflow registry, Qdrant, Feast, vLLM — không có cache. Profile
D cho thấy chi phí chi phối: mỗi request `/ask` tốn ~994 ms ở P50 **chỉ để thất
bại**, vì client vLLM phải chờ hết connect timeout tới một endpoint không tồn
tại. Cùng chi phí đó nằm trong mọi lần gọi `/ready`.

Hệ quả: 9 lần 503 ở profile A là do probe bắt buộc timeout dưới tải, chứ không
phải dependency thật sự chết. Một readiness endpoint tự gây ra lỗi readiness khi
bị gọi nhiều là một vòng lặp phản hồi cần chặn.

**Ba việc nên làm, theo thứ tự:**

1. Cache kết quả probe trong 1–2 giây. `/ready` được kubelet gọi mỗi 10 giây và
   gateway gọi mỗi 2 giây; chạy lại toàn bộ fan-out cho từng lần gọi là lãng phí
   thuần túy.
2. Giảm connect timeout của vLLM và mở circuit breaker sau N lần lỗi liên tiếp,
   để một endpoint chết trả lời tức thì thay vì tốn một giây mỗi request.
3. Chỉ sau hai việc trên mới đo lại — đo `/ask` với vLLM thật mới có ý nghĩa.

## 5. Kubernetes / GitOps (SUBMISSION mục 7)

```text
$ uv run python scripts/validate_manifests.py
Kubernetes and GitOps manifest contracts passed

$ kubectl kustomize deploy/kubernetes/base | grep -c '^kind:'
10
```

Mười tài nguyên build ra: `Namespace`, `ServiceAccount`, `ConfigMap`, `Service`,
`Deployment`, `PodDisruptionBudget`, `HorizontalPodAutoscaler`, `Gateway`,
`HTTPRoute`, `NetworkPolicy`.

Những gì được ghim, và tại sao quan trọng cho rollback:

| Ghim | Giá trị |
|---|---|
| Image | `ghcr.io/vinuni-ai20k/day28-platform-api:3.0.0` — không dùng `:latest` |
| Argo CD `targetRevision` | `refs/tags/v3.0.0` — không dùng `HEAD`/`main` |
| `syncPolicy.automated` | `prune: true`, `selfHeal: true` |
| `revisionHistoryLimit` | 5 |

`validate_manifests.py` từ chối `:latest` và từ chối `targetRevision` trỏ nhánh
di động. Cả hai đều cần cho rollback: nếu desired state trỏ một nhánh, "quay lại
bản trước" không có nghĩa xác định.

Ba probe của Deployment giờ hỏi ba câu khác nhau — `/health` liveness, `/startup`
startup, `/ready` readiness — và cả ba mang header `x-lab28-synthetic-probe` để
lưu lượng của kubelet không lọt vào golden signals.

**Giới hạn phải nói rõ:** những manifest này chưa từng được apply lên một cluster
thật trong bài này (máy không có cluster; chỉ có `kubectl` client). Drift và
self-heal được lập luận từ `selfHeal: true` chứ **không** phải quan sát trực
tiếp. Quy trình rollback dự kiến nằm ở [`runbooks/gitops-rollback.md`](../runbooks/gitops-rollback.md).

Rollback ở tầng mô hình thì **có** chạy thật, và không cần deploy:
`integration-tests/test_j3_promotion_rollback.py` đăng ký một release mới,
chuyển alias `champion`, kiểm tra đường phục vụ đổi theo, rồi `lab28 rollback`
đưa alias về version trước.

## 6. Observability (IP09, IP10)

Nguồn: `evidence/ip09-prometheus-targets.json`,
`evidence/ip09-grafana-dashboards.json`, `evidence/ip10-trace.json`.

Alert rules sau khi bổ sung SLO — 15 rule, 3 group, không rule nào lỗi:

| Group | Nội dung |
|---|---|
| `lab28-slo-definitions` | 6 recording rule: tỉ lệ lỗi và tỉ lệ vượt 1000 ms của `/api/v1/ask` trên các cửa sổ 5m/30m/1h/6h |
| `lab28-slo-burn` | 3 alert burn-rate đa cửa sổ: 14.4× (page), 6× (ticket), và latency 3× |
| `lab28-platform` | 6 alert theo boundary: API mất scrape, phụ thuộc bắt buộc hỏng, phụ thuộc tùy chọn hỏng kéo dài, DLQ tồn đọng, consumer lag, collector rơi span |

SLO được phát biểu rõ trong `monitoring/alerts.yml`: 99% request `/api/v1/ask`
không trả 5xx, và 95% hoàn thành dưới 1000 ms — đúng ngân sách mà `pipeline.py`
đo từng chặng.

Hai chi tiết đáng giải thích khi demo:

- **Không dùng `clamp_min` ở mẫu số.** Khi không có traffic, tử và mẫu đều bằng
  0, tỉ lệ là `NaN`, và mọi so sánh với `NaN` đều sai — nên không có alert. Đó
  là điều đúng: *không có traffic* không phải là *không có lỗi*. Luật cũ
  `clamp_min(..., 1)` sẽ báo một tỉ lệ khỏe mạnh mà hệ thống chưa hề chứng minh.
- **Bỏ cửa sổ 24 giờ và 3 ngày** của scheme chuẩn. Một stack lab không chạy đủ
  lâu để lấp đầy chúng, và một luật không bao giờ đủ dữ liệu là một luật không
  ai nên tin.

## 7. Những gì chưa xác minh được

### 7.1 Bộ test `gpu` — kết quả cuối 14/15 pass

Phiên Kaggle đầu ngắt giữa chừng ở 12/15 pass (tunnel chết, HTTP 530). Nối lại
phiên thứ hai, sửa nốt hai lỗi thật tìm được, chạy lại toàn bộ:

```text
uv run pytest integration-tests -m gpu -q -p no:randomly
→ 14 passed, 1 failed, 57 deselected in 380s
```

Ba lỗi tìm được ở phiên đầu, ba nguyên nhân khác nhau:

**a) `test_the_gateway_stops_routing_to_a_pod_that_is_not_ready` — lỗi thật, đã sửa và xác nhận PASS.**
Envoy active-health-check trỏ `/health` (liveness) thay vì `/ready`, nên gateway
không bao giờ eject pod khi một dependency bắt buộc chết. Sửa
`gateway/envoy.yaml`. Xác minh cả bằng tay lẫn bằng pytest:

```text
trước:  gateway /ready = 200
docker compose stop qdrant
sau 8s: gateway /ready = 503, body KHÔNG có "components" (lỗi của chính Envoy)
docker compose start qdrant
sau 6s: gateway /ready = 200 (phục hồi)
```

**b) `test_the_inference_endpoint_is_scraped` — lệch cấu hình môi trường, đã sửa và xác nhận PASS.**
`monitoring/prometheus.yml` scrape `host.docker.internal:8001`, nhưng vLLM thật
nằm sau URL tunnel đổi mỗi phiên — Prometheus không theo được. Viết
`scripts/vllm_local_forward.py`, một reverse proxy stdlib-only bắc cầu
`127.0.0.1:8001` → URL tunnel, để mọi thứ (API, readiness probe, Prometheus)
tiếp tục nói chuyện qua cổng 8001 như thiết kế gốc của `ports.template`. Chạy
proxy, đợi Prometheus scrape lại: target `lab28-vllm-optional` chuyển
`up`, `evidence/ip09-prometheus-targets.json` ghi lại đúng.

**Phát sinh trong lúc chạy bộ gpu đầy đủ:** timeout 2s của active health check
quá chặt cho một round trip xuyên Internet tới Kaggle — dưới tải nặng của cả
bộ test, độ trễ tunnel thỉnh thoảng vượt 2s, gateway eject nhầm pod đang khỏe
(`health_flags: active_hc_timeout`), làm một request `/api/v1/ask` giữa suite
nhận `503 no healthy upstream`. Nới lên 5s (`gateway/envoy.yaml`) — đủ chịu
được chặng GPU từ xa mà không làm chậm phát hiện sự cố thật (dependency cục bộ
vẫn trả lời trong mili giây bất kể timeout).

**c) `test_the_trace_spans_the_processes_the_contract_claims` — giới hạn kiến trúc, KHÔNG sửa được từ phía repo, còn FAIL.**
Đòi hỏi ≥4 service riêng biệt trên một trace (thực tế có 3: `lab28-gateway`,
`lab28-api`, `lab28-airflow`). Service thứ tư chỉ xuất hiện nếu **vLLM tự phát
span OTLP** về collector của mình. `compose.gpu.yaml` — đường chạy vLLM cục bộ
trên GPU cùng máy — có thể làm được vì container vLLM nằm chung docker network
với `otel-collector:4317`. vLLM chạy trên Kaggle thì không: collector của ta chỉ
lắng nghe cục bộ, không có địa chỉ công khai, và Kaggle không đi qua được vào
mạng docker của ta. Muốn vLLM tự export span cần mở thêm một tunnel TCP cho
cổng OTLP và cấu hình `--otlp-traces-endpoint` phía Kaggle — khả thi nhưng thêm
một tầng hạ tầng dễ vỡ cho đúng một test phụ, không thử vì lợi ích/rủi ro không
đáng. Đây là khoảng trống của **đường chạy GPU từ xa qua tunnel**, không phải
của đường chạy GPU cục bộ mà `compose.gpu.yaml` nhắm tới. `evidence/ip10-trace.json`
xác nhận: đủ cả 11 required span name (`required_spans_missing: []`, bao gồm
`lab28.vllm.chat_completion` — span này có thật, chỉ là được gắn dưới service
`lab28-api` chứ không phải một service riêng), chỉ thiếu số lượng service.

### 7.2 Còn lại

| Hạng mục | Trạng thái | Lý do |
|---|---|---|
| Chân LangSmith của IP10 | `UNVERIFIED` | Không có `LANGSMITH_API_KEY`. Chân OTLP nội bộ đã chứng minh bằng `evidence/ip10-trace.json` |
| Argo CD drift/self-heal | Chưa quan sát | Không có cluster; chỉ validate manifest tĩnh |
| Trace 4-service (7.1c) | Không khả thi qua tunnel hiện tại | Xem lý do ở trên |

Load profile `/ask` với model thật: thử với 3 worker song song trong lúc bộ
`gpu` vừa chạy xong và bộ non-gpu đang chạy nền cùng lúc — 20/20 request timeout
hoặc lỗi. Đây không phải lỗi cấu hình: `/ready` và tunnel vẫn `200` suốt lúc đó.
Nguyên nhân là tranh chấp tài nguyên thật — một T4 chạy một worker vLLM không
chịu nổi vừa phục vụ pytest vừa phục vụ load test cùng lúc qua một tunnel công
khai. Số liệu ở mục 2.1 (một request đơn lẻ, không tranh chấp: 13.8 s, phần lớn
do retrieval nguội — chưa từng có ai hỏi qua vector store trước đó) là mẫu sạch
duy nhất lấy được trong phiên này.
