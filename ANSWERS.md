# ANSWERS — Day 28 Track 2

Người thực hiện: **Tạ Đăng Đức** (2A202601772) — làm **cá nhân**, nhánh
`ca-nhan-ductd`. Một người đảm nhiệm lần lượt cả năm vai trò trong
[`docs/team-role-cards.md`](docs/team-role-cards.md).

Bằng chứng số liệu nằm ở [`docs/evidence-pack.md`](docs/evidence-pack.md) và thư
mục [`evidence/`](evidence/). Tài liệu này chỉ trả lời phần lập luận mà
[`SUBMISSION.md`](SUBMISSION.md) mục 8 yêu cầu.

## 1. Bốn hàm đã hoàn thiện và lý do chọn cách làm

Toàn bộ nằm trong
[`src/lab28_platform/integration_tasks.py`](src/lab28_platform/integration_tasks.py).
Bốn hàm này được hệ thống thật gọi trực tiếp (Kafka producer, Spark MERGE, Feast
client, `/ready`), nên mỗi lựa chọn ở đây là một quyết định vận hành chứ không
phải một bài tập tách rời.

### `event_headers` — IP01 + IP10

Header luôn có `idempotency-key`, và chỉ có `traceparent` khi thật sự đang có
span hoạt động.

Điểm đánh đổi: gửi `traceparent=""` sẽ làm code phía consumer đơn giản hơn (luôn
có key để đọc), nhưng chuỗi rỗng không phải là một W3C traceparent hợp lệ. Một
collector đúng chuẩn sẽ hoặc bỏ qua, hoặc tệ hơn là bắt đầu một trace mới và cắt
đứt chuỗi liên kết ở đúng chỗ khó chẩn đoán nhất — ranh giới bất đồng bộ. Vì vậy
"không có trace" được biểu diễn bằng **vắng mặt header**, đúng như đặc tả W3C.

### `dedupe_latest` — IP03

Duyệt danh sách đúng một lần, giữ bản ghi có `(occurred_at, event_id)` lớn nhất
cho mỗi `idempotency_key`, rồi trả về theo thứ tự `idempotency_key` đã sắp xếp.

Ba đánh đổi:

- **So sánh cặp, không so sánh riêng `occurred_at`.** Hai sự kiện có thể trùng
  timestamp tới micro giây; nếu chỉ so timestamp thì bản được giữ lại phụ thuộc
  vào thứ tự Kafka giao hàng, mà thứ tự đó không ổn định giữa các lần replay.
  `event_id` là tie-breaker tất định.
- **Sắp xếp đầu ra.** Không cần cho tính đúng đắn của `MERGE`, nhưng làm cho một
  lô đầu vào luôn sinh ra cùng một nguồn MERGE, nên khi so sánh hai lần chạy chỉ
  còn khác biệt thật sự chứ không phải khác biệt thứ tự.
- **Gom trong bộ nhớ theo lô.** Đơn giản và đủ cho kích thước lô của lab; giới
  hạn là lô phải vừa RAM của worker. Ở quy mô thật, phần này nên đẩy xuống một
  window function trong Spark (`row_number()` theo `idempotency_key`).

Chống trùng có hai lớp: hàm này khử trùng **trong** một lô, còn `MERGE ... WHEN
MATCHED` khử trùng **giữa** các lô. Thiếu lớp nào thì replay cũng sinh bản ghi
trùng.

### `feast_online_request` — IP04

Danh sách feature lấy từ `FEATURE_REFS` trong
[`contracts.py`](src/lab28_platform/contracts.py) chứ không viết lại tại chỗ.
Đây là điểm đánh đổi rõ nhất giữa "đọc code nhanh" và "không lệch hợp đồng": một
danh sách feature chép tay sẽ âm thầm sai khi feature view đổi tên, và Feast trả
`NOT_FOUND` cho từng feature thay vì báo lỗi cấu hình. Một nguồn sự thật duy
nhất khiến sai lệch đó thành lỗi import chứ không thành lỗi runtime.

`full_feature_names=False` để khóa trả về là tên feature trần
(`feedback_count`), khớp với schema `AskerFeatures` phía client.

### `readiness_status` — IP07 + IP08

Thứ tự ưu tiên: một probe `mandatory` hỏng → `not_ready`; chỉ probe không bắt
buộc hỏng → `degraded`; còn lại → `ready`.

Đây là quyết định có hậu quả trực tiếp lên tính sẵn sàng: `/ready` trả 503 sẽ
làm Envoy/Kubernetes rút pod khỏi vòng quay. Nếu coi mọi phụ thuộc là bắt buộc,
một Feast lạnh sẽ hạ toàn bộ pod đang phục vụ được — tự gây sự cố lớn hơn sự cố
gốc. Vì vậy Feast được khai báo `mandatory=False` trong
[`readiness.py`](src/lab28_platform/readiness.py): feature lạnh làm câu trả lời
kém đi, không làm nó sai.

`degraded` là trạng thái hạng nhất chứ không phải `ready` có chú thích: nó vẫn
nhận traffic, nhưng xuất hiện khác biệt trên dashboard và trong
`evidence.degraded_reasons` của từng câu trả lời.

## 2. Đánh đổi kiến trúc

| Quyết định | Đã chọn | Đánh đổi chấp nhận |
|---|---|---|
| Ranh giới bất đồng bộ | HTTP nhận rồi trả `202`, Kafka mang phần còn lại | Client không biết ngay dữ liệu đã vào Delta; bù lại ingestion không sập theo Spark/Airflow |
| Chống trùng | Khóa do client cấp, hoặc suy ra từ nội dung (`_derive_key`) | Hai văn bản khác nhau về ngữ nghĩa nhưng giống hệt chuỗi sẽ bị gộp; đổi lại retry của client là an toàn |
| Commit offset | Chỉ commit **sau** khi ghi Delta thành công | Crash giữa chừng gây replay (chấp nhận được vì MERGE idempotent), thay vì mất dữ liệu |
| Poison message | DLQ sau số lần thử có giới hạn, rồi tiến offset | Cần thao tác thủ công để replay DLQ; đổi lại một bản tin hỏng không chặn partition |
| Suy giảm dịch vụ | Feast/Qdrant có đường suy giảm, vLLM thì không | Không có vLLM là 503 thật, vì một câu trả lời không có mô hình là câu trả lời bịa |
| Ngân sách độ trễ | Vượt ngân sách **đếm metric**, không cắt request | Đuôi độ trễ dài vẫn lọt qua; đổi lại không cắt ngang một câu trả lời gần xong |
| Rate limit | Local rate limit tại Envoy (10 rps/instance) | Không chính xác toàn cục khi scale nhiều instance; đổi lại không cần Redis và không có điểm chết mới |
| Health tại gateway | `/healthz` là `direct_response` của Envoy | Không phản ánh sức khỏe ứng dụng — đúng chủ ý: liveness của gateway không được phụ thuộc upstream |
| Promotion mô hình | Alias `champion` trong MLflow, không sửa code | Rollback là một thao tác registry (giây), không phải một lần deploy |
| Cài đặt Python | `--no-editable` | Sửa code phải build lại image API; đổi lại hành vi giống nhau trên Windows/macOS/Linux |

Hai quyết định đáng nói riêng:

**Metric không đếm lưu lượng thăm dò tổng hợp.** Envoy health-check cluster mỗi
2 giây vào `/health`, Docker healthcheck mỗi 5 giây nữa. Nếu đếm những request
đó vào `lab28_requests_total` thì request rate bị thổi phồng, độ trễ p50 bị kéo
về 0, và series `route="/health"` không còn nói gì về client thật. Gateway,
Docker và kubelet nay đều gắn header `x-lab28-synthetic-probe`, và middleware
trong [`api.py`](src/lab28_platform/api.py) bỏ qua các request mang header đó.

**Client phải tôn trọng rate limit, không phải rate limit phải nhường client.**
`lab28 seed --via-gateway` gửi 25 request vào một limiter 10 rps nên luôn bị 429
ở phần đuôi. Cách sửa sai là nới limiter — nhưng IP08 tồn tại để chứng minh
limiter có bắn. Nên phần sửa nằm ở phía client: gặp 429 thì đợi rồi thử lại, và
chỉ 429 mới được thử lại (4xx khác sẽ lặp lại y hệt mãi mãi, 5xx là sự cố cần
người nhìn thấy). Submission vốn idempotent nên thử lại không tốn gì.

Danh sách đầy đủ sáu lỗi đã sửa ở
[`docs/evidence-pack.md`](docs/evidence-pack.md) mục 1.

## 3. Khoảng cách so với production

Những điều đúng trong lab này nhưng **chưa đủ** để chạy thật:

**Dữ liệu và trạng thái**

1. Kafka một broker, `replication_factor=1`. Mất broker là mất dữ liệu chưa
   xử lý. Production cần tối thiểu 3 broker và `min.insync.replicas=2`.
2. Delta chạy trên volume cục bộ, chưa có compaction/`VACUUM` định kỳ. Sau vài
   nghìn lần MERGE, số file nhỏ sẽ làm chậm cả đọc lẫn ghi.
3. Feast online store là SQLite (`.lab28/feast/online_store.db`). Đúng cho một
   máy, sai cho nhiều replica: production cần Redis hoặc DynamoDB.
4. Chưa có backfill/reprocessing có kiểm soát. Replay hiện là "phát lại DLQ";
   chưa có cách chạy lại một khoảng thời gian.
5. Metadata database của Airflow là một file SQLite, trong khi executor là
   `LocalExecutor` với `parallelism=4`. Airflow chỉ hỗ trợ SQLite cho
   `SequentialExecutor`; ở đây scheduler, triggerer, dag-processor và các task
   cùng ghi vào một file. Tôi đã nâng busy timeout lên 60 giây để hết lỗi
   `database is locked` (xem mục "Sửa chữa" trong evidence pack), nhưng đó là
   biện pháp giảm nhẹ. Production phải dùng PostgreSQL.

**Mô hình và phục vụ**

6. IP07 chưa xác minh được trong môi trường này (không có endpoint vLLM thật) —
   báo `UNVERIFIED`, không giả lập. Xem mục 4 dưới đây.
7. Chưa có canary/shadow. Promotion là chuyển alias tức thời cho 100% traffic;
   production cần chia phần trăm và tự động rollback theo metric.
8. Guardrails là redaction theo regex, tác giả đã ghi rõ là bản dạy học. Nó bắt
   được email/số điện thoại theo mẫu, không bắt được PII dạng tự do.
9. Chưa có đánh giá chất lượng liên tục. `lab28 release` ghi nhận một lần đánh
   giá; không có gì phát hiện chất lượng trả lời trôi đi sau đó.

**Vận hành và bảo mật**

10. Không có xác thực ở gateway. Envoy định tuyến mọi request; production cần
   OIDC/mTLS và phân quyền theo route.
11. Rate limit là local per-instance, không phải per-tenant toàn cục.
12. Alert đã bổ sung SLO burn-rate đa cửa sổ (14.4× page, 6× ticket) cho
    availability và latency của `/api/v1/ask`, cộng sáu alert theo boundary.
    Còn thiếu: cửa sổ 24 giờ/3 ngày của scheme chuẩn — lab không chạy đủ lâu để
    lấp đầy, và chưa có Alertmanager để định tuyến theo nhãn `owner`.
13. Chưa có quản lý bí mật. Cấu hình đi qua biến môi trường trong Compose;
    production cần external secret store và xoay vòng khóa.
14. Manifest Kubernetes mới được kiểm bằng `validate_manifests.py` và
    `kubectl kustomize`, chưa từng được apply lên một cluster thật trong bài
    này; drift/self-heal của Argo CD được lập luận từ cấu hình chứ chưa quan
    sát trực tiếp.
15. `NetworkPolicy` chỉ cho phép ingress vào cổng 8000 từ namespace của gateway
    controller. Khi thêm Prometheus ở namespace khác, scrape `/metrics` sẽ bị
    chặn; cần một rule ingress riêng cho namespace giám sát.

## 4. Các gate theo môi trường

| Gate | Trạng thái | Lý do |
|---|---|---|
| `gpu` (IP07 — vLLM thật) | **UNVERIFIED** | Máy chạy bài không có GPU NVIDIA và lớp chưa cấp endpoint vLLM. `probe_identity` báo `unreachable: ConnectError`, nên `lab28 ready` trả `not_ready` khi `LAB28_VLLM_REQUIRE_REAL=true`. Đúng theo thiết kế: một server chỉ tương thích OpenAI mà không chứng minh được `/version`, `/v1/models` và metric `vllm:` thì **không** được tính là đạt. Không dựng server giả. |
| `langsmith` (chân LangSmith của IP10) | **UNVERIFIED** | Không có `LANGSMITH_API_KEY`. Chân OTLP cục bộ (collector → Jaeger) vẫn được kiểm và có bằng chứng ở `evidence/ip10-trace.json`. |

Hai gate này được đánh dấu trong `contracts/integration-matrix.yaml` và loại
khỏi lần chạy bằng `-m "not gpu and not langsmith"`, đúng như
[`SUBMISSION.md`](SUBMISSION.md) hướng dẫn.

## 5. Phân công và đóng góp

Bài làm cá nhân, nên bảng dưới ghi thứ tự thực hiện theo từng vai trò thay vì
chia người:

| Vai trò | Điểm kết nối | Đã làm |
|---|---|---|
| Ingestion & Orchestration | IP01–IP02 | Hoàn thiện `event_headers`; chạy `lab28 topics`, `seed`, DAG `lab28_ingestion_pipeline`; kiểm tra header trace trên `data.raw` |
| Data & ML | IP03–IP04–IP06 | Hoàn thiện `dedupe_latest`, `feast_online_request`; xác minh MERGE idempotent, time travel, materialize Feast, alias `champion` và rollback |
| Serving & Retrieval | IP05–IP07 | `lab28 index` với ID tất định; kiểm tra đường suy giảm; IP07 dừng ở `UNVERIFIED` có lý do |
| Platform & Observability | IP08–IP10 | Sửa lỗi metric của health-check gateway; sửa healthcheck Airflow báo sai; thêm SLO burn-rate alert; kiểm tra 200/429 + `x-request-id`, target Prometheus, dashboard Grafana, độ phủ span |
| Presenter / Incident Commander | — | Chạy hai kịch bản sự cố có ghi giả thuyết trước, đo load profile bốn cấu hình, viết `docs/evidence-pack.md` và tài liệu này |

## 6. Ba câu hỏi tôi chuẩn bị cho phần Q&A

**Tại sao `/health` và `/ready` phải tách?** `/health` trả lời "tiến trình này
có còn sống không" và cố tình không chạm vào phụ thuộc nào — nếu nó gọi Kafka
thì một Kafka chậm sẽ làm Kubernetes **khởi động lại** ứng dụng đang khỏe.
`/ready` trả lời "pod này có nên nhận request không" và bắt buộc phải chạm phụ
thuộc, vì đó chính là câu hỏi được đặt ra.

**Chứng minh replay không mất và không nhân đôi dữ liệu như thế nào?** Ghi lại
số dòng và version của bảng Delta trước khi replay, phát lại đúng lô đó, rồi so
lại: số dòng không đổi, version tăng (một commit MERGE mới không thêm dòng nào),
và `time_travel_evidence` cho thấy khác biệt giữa hai version là rỗng về phía
dữ liệu. Số dòng không đổi chứng minh không nhân đôi; commit mới tồn tại chứng
minh lô đã thật sự được xử lý chứ không bị bỏ qua âm thầm.

**Rollback nhanh nhất có thể là bao lâu?** Bằng thời gian MLflow đổi alias
`champion` sang version trước, cộng thời gian TTL cache release trong tiến trình
phục vụ — vì đường phục vụ đọc alias chứ không nhúng version vào code. Không cần
build lại image, không cần deploy.
