# Day 28 Track 2 — Bài thực hành kết nối nền tảng AI

> **Bắt đầu ở đây.** Kho mã đã có sẵn phần khung. Bạn cần hoàn thiện 4 chức năng
> quan trọng, kiểm tra kết quả sau từng bước và chuẩn bị trình bày 10 điểm kết
> nối của hệ thống trước lớp. Có thể làm **cá nhân hoặc theo nhóm**.

## Bạn sẽ làm được gì?

Sau bài thực hành, bạn có thể:

1. truyền mã theo dõi và khóa chống ghi trùng qua Kafka;
2. giữ dữ liệu Delta không bị trùng khi Kafka gửi lại bản tin;
3. tạo đúng yêu cầu giữa API và Feast;
4. phân biệt `ready`, `degraded` và `not_ready`;
5. chạy, theo dõi và giải thích một nền tảng AI gồm Kafka, Airflow, Spark/Delta,
   Feast, Qdrant, MLflow, vLLM, Envoy và OpenTelemetry;
6. trình bày luồng chạy đúng, cách khôi phục sau một sự cố và cách quay lại phiên
   bản trước.

Không cần chạy mọi thành phần trên một laptop. Phần viết mã và kiểm thử chạy được
trên Windows, macOS và Linux. Toàn bộ hệ thống có thể chạy trên máy cá nhân, máy
chung của nhóm hoặc môi trường do giảng viên cung cấp; GPU có thể dùng Kaggle hay
máy dùng chung.

### Một số từ sẽ gặp trong bài

| Từ trong mã hoặc công cụ | Hiểu đơn giản là |
|---|---|
| `IP01` … `IP10` | 10 điểm mà các thành phần của hệ thống kết nối với nhau |
| `trace` / `trace ID` | mã dùng để theo dõi một yêu cầu khi nó đi qua nhiều thành phần |
| `idempotency key` | khóa giúp cùng một dữ liệu gửi lại nhiều lần nhưng chỉ được ghi một lần |
| `replay` | Kafka gửi lại bản tin đã từng gửi |
| `ready` / `degraded` / `not_ready` | sẵn sàng / vẫn chạy nhưng thiếu một phần / chưa thể nhận yêu cầu |
| `evidence` | kết quả lệnh hoặc ảnh màn hình dùng để chứng minh phần demo |

## Kiến trúc và 10 điểm kết nối

![Sơ đồ kiến trúc trực quan của bài thực hành](docs/images/lab28-architecture-overview.png)

Đọc ảnh theo ba vùng màu:

1. **Luồng chính:** yêu cầu đi từ người dùng qua Envoy, FastAPI, Kafka, Airflow
   rồi được ghi vào Delta Lake.
2. **Dữ liệu và mô hình:** Delta cung cấp dữ liệu cho Feast, Qdrant và MLflow;
   FastAPI gọi vLLM để tạo câu trả lời.
3. **Giám sát:** Prometheus/Grafana theo dõi số liệu, còn
   OpenTelemetry/Jaeger giúp lần theo một yêu cầu từ đầu đến cuối.

Bạn chưa cần nhớ ngay mọi công nghệ. Hãy bắt đầu bằng việc chỉ theo các mũi tên
IP01–IP10; mỗi mũi tên là một điều cần kiểm tra và giải thích khi trình bày.

Danh sách 10 yêu cầu dùng để kiểm tra kết quả nằm ở
[`contracts/integration-matrix.yaml`](contracts/integration-matrix.yaml). Mô tả
đầy đủ của bài toán nằm ở [`LAB28.md`](LAB28.md); thang điểm nằm ở
[`docs/rubric.md`](docs/rubric.md).

## Chọn đường chạy phù hợp với máy

| Đường chạy | Bạn cần | Làm được gì | Phù hợp |
|---|---|---|---|
| **Chỉ viết mã** | 4 GB RAM, khoảng 3 GB trống | 4 chỗ cần hoàn thiện, kiểm thử nhanh và kiểm tra tệp cấu hình | Mọi máy |
| **Hệ thống cơ bản** | Khuyến nghị 8 GB RAM, 4 CPU, 12 GB trống | Kafka, API, cổng truy cập, Feast, Qdrant, MLflow và màn hình theo dõi | Laptop trung bình |
| **Toàn bộ hệ thống** | Khuyến nghị 12–16 GB RAM, 6 CPU, 20 GB trống | Thêm Spark Connect, Airflow và 5 luồng kiểm thử thực tế | Máy đủ mạnh hoặc máy dùng chung |
| **Phần GPU** | NVIDIA phù hợp, Kaggle T4 hoặc máy GPU thật | Nối hệ thống với vLLM thật | Không bắt buộc chạy riêng trên mỗi máy |

Nếu lệnh `preflight` đề nghị `browser-fallback`, bạn vẫn đạt đủ mục tiêu bài học:
làm Bước 1–6 trên máy mình, sau đó dùng hệ thống chung để chạy Bước 7–9. Kaggle
chỉ cung cấp GPU cho IP07/vLLM; nó không thay Kafka, Delta, Feast hay phần giám
sát. Xem [`KAGGLE_GPU_EXTENSION.md`](KAGGLE_GPU_EXTENSION.md).

## Trước khi bắt đầu

Cài ba công cụ:

- Git;
- `uv` theo [hướng dẫn chính thức](https://docs.astral.sh/uv/getting-started/installation/);
- Docker Desktop trên Windows/macOS, hoặc Docker Engine + Compose plugin trên
  Linux, theo [hướng dẫn chính thức](https://docs.docker.com/engine/install/).

Kiểm tra ở PowerShell, Terminal hoặc shell Linux:

```text
git --version
uv --version
docker version
docker compose version
```

`uv` sẽ tự cài Python 3.11 nếu máy chưa có. Không cần tự bật môi trường Python
ảo và không cần `make`, nên các lệnh dưới đây giống nhau trên cả ba hệ điều hành.

## Bước 1 — Tải kho mã và tạo nhánh làm việc

```text
git clone https://github.com/VinUni-AI20k/Day28-Modern-Platform-Lab-Student.git
cd Day28-Modern-Platform-Lab-Student
```

Nếu làm cá nhân:

```text
git switch -c ca-nhan-<ten-ngan>
```

Nếu làm theo nhóm:

```text
git switch -c nhom-<so-nhom>
```

Ví dụ: `ca-nhan-an` hoặc `nhom-03`. Mỗi người vẫn có thể tạo nhánh riêng rồi
gộp mã vào nhánh của nhóm.

**Tự kiểm tra:** `git status` hiển thị đúng nhánh vừa tạo và chưa có tệp bị sửa.

## Bước 2 — Cài môi trường Python

```text
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
uv run lab28 --help
uv run lab28 preflight
```

`--no-editable` tránh khác biệt filesystem/permission ở thư mục đồng bộ trên
Windows, macOS và Linux. Mỗi lần sửa code, `uv run` vẫn đọc `src/` nhờ cấu hình
project.

### Kết quả mong đợi

- `lab28 --help` hiển thị các command như `preflight`, `topics`, `seed`, `ready`;
- `preflight` in kết quả có `profile`, `python`, `docker_daemon`, `memory_gib` và
  `next`;
- `profile=local-standard`: có thể thử hệ thống cơ bản hoặc toàn bộ hệ thống;
- `profile=browser-fallback`: tiếp tục phần viết mã, không cố ép Docker chạy.

## Bước 3 — Chạy 4 kiểm thử chưa đạt ban đầu

```text
uv run pytest starter-tests -q
```

### Kết quả mong đợi

Đúng **4 test fail** với `NotImplementedError`, tương ứng 4 hàm trong
[`src/lab28_platform/integration_tasks.py`](src/lab28_platform/integration_tasks.py):

| Hàm cần hoàn thiện | Điểm kết nối | Điều được kiểm tra |
|---|---|---|
| `event_headers` | IP01 + IP10 | mã theo dõi và khóa chống trùng cùng đi qua Kafka |
| `dedupe_latest` | IP03 | dữ liệu gửi lại không tạo bản ghi trùng; bản mới nhất được giữ |
| `feast_online_request` | IP04 | tên đối tượng và đặc trưng đúng với Feast |
| `readiness_status` | IP07 + IP08 | lỗi bắt buộc và lỗi không bắt buộc cho đúng trạng thái |

Đây là kết quả ban đầu đúng. Không sửa hoặc xóa kiểm thử, và không che lỗi
`NotImplementedError`.

## Bước 4 — Chọn cách làm cá nhân hoặc theo nhóm

Đọc [`docs/team-role-cards.md`](docs/team-role-cards.md). Nếu làm theo nhóm, chia
các phần sau cho từng người. Nếu làm cá nhân, dùng chúng như một danh sách để tự
kiểm tra rằng mình không bỏ sót phần nào:

- Ingestion & Orchestration: IP01–IP02;
- Data & ML: IP03–IP04–IP06;
- Serving & Retrieval: IP05–IP07;
- Nền tảng và giám sát: IP08–IP10;
- Trình bày: chuẩn bị bằng chứng, thứ tự demo và trả lời câu hỏi.

Nhóm ít người có thể kiêm nhiều phần. Khi làm nhóm, mỗi thành viên cần hiểu luồng
từ đầu đến cuối. Khi làm cá nhân, một người lần lượt thực hiện đủ các phần trên.

## Bước 5 — Hoàn thiện 4 chức năng

Chỉ sửa
[`src/lab28_platform/integration_tasks.py`](src/lab28_platform/integration_tasks.py)
ở vòng đầu. Các phần còn lại của hệ thống đã gọi trực tiếp bốn hàm này, nên mã
bạn viết sẽ được dùng trong luồng chạy thật của bài thực hành.

### Phần A — Thông tin đi kèm bản tin Kafka (IP01 + IP10)

Yêu cầu:

- luôn trả `idempotency-key` dạng `bytes`;
- có mã theo dõi thì trả `traceparent` dạng `bytes`;
- không có mã theo dõi thì **bỏ mục này**, không gửi chuỗi rỗng;
- không viết cố định khóa hay mã theo dõi trong mã nguồn.

```text
uv run pytest starter-tests/test_integration_tasks.py -k event_headers -q
```

**Đạt khi:** `1 passed, 3 deselected`.

### Phần B — Loại bản ghi trùng khi Kafka gửi lại dữ liệu (IP03)

Yêu cầu:

- đọc toàn bộ danh sách đầu vào đúng một lần;
- giữ một bản tin cho mỗi `idempotency_key`;
- bản tin có cặp `(occurred_at, event_id)` lớn nhất được giữ lại;
- sắp xếp kết quả theo `idempotency_key` để mỗi lần chạy cho cùng thứ tự;
- đầu vào rỗng trả về danh sách rỗng.

```text
uv run pytest starter-tests/test_integration_tasks.py -k delta_source -q
uv run pytest tests/test_delta_merge_idempotency.py -q
```

**Đạt khi:** kiểm thử riêng và toàn bộ kiểm thử Delta đều đạt. Nếu kiểm thử riêng
đạt nhưng kiểm thử Delta lỗi, mã chưa xử lý đúng đối tượng `IngestionEvent`.

### Phần C — Tạo yêu cầu đọc dữ liệu từ Feast (IP04)

Yêu cầu phần dữ liệu gửi đến Feast:

- `entities = {"asker_id": [asker_id]}`;
- bốn đặc trưng của `asker_activity_v1`;
- `full_feature_names = false`;
- lấy danh sách chuẩn từ
  [`src/lab28_platform/contracts.py`](src/lab28_platform/contracts.py), không tự
  viết lại cùng một danh sách ở nhiều nơi.

```text
uv run pytest starter-tests/test_integration_tasks.py -k feast_request -q
```

**Đạt khi:** `1 passed, 3 deselected`.

### Phần D — Xác định mức sẵn sàng của hệ thống (IP07 + IP08)

Thứ tự ưu tiên:

1. có ít nhất một phép kiểm tra `mandatory=true` bị lỗi → `not_ready`;
2. phần bắt buộc không lỗi nhưng phần không bắt buộc bị lỗi → `degraded`;
3. còn lại → `ready`.

```text
uv run pytest starter-tests/test_integration_tasks.py -k readiness -q
```

**Đạt khi:** `1 passed, 3 deselected`.

### Kiểm tra tổng sau 4 phần

```text
uv run pytest starter-tests tests -q
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
```

### Kết quả mong đợi

- không còn `NotImplementedError`;
- kiểm thử ban đầu và kiểm thử nhanh đều không có lỗi;
- danh sách có đủ 10 điểm kết nối và không tham chiếu kiểm thử bị thiếu;
- kiểm tra đa hệ điều hành và tệp triển khai đều trả mã `0`;
- Ruff không có lỗi.

Nếu lần kiểm tra này chưa đạt, chưa chuyển sang Docker.

## Bước 6 — Kiểm tra cấu hình Docker trước khi tải image

```text
docker compose --env-file ports.template config --quiet
docker compose --env-file ports.template --profile full config --quiet
```

Không in lỗi và trả mã `0` nghĩa là YAML, chế độ chạy và cổng mạng hợp lệ. File
`ports.template` chỉ chứa cổng và tên mô hình mặc định, không chứa mật khẩu.

Nếu cổng trùng, sao chép file đó thành một file cấu hình riêng, đổi **chỉ cổng
trên máy**, rồi thay đường dẫn sau `--env-file` trong mọi lệnh. Không đưa token,
mật khẩu hoặc URL bí mật lên Git.

## Bước 7 — Chạy hệ thống cơ bản

Chỉ chạy nếu `preflight` cho phép hoặc giảng viên yêu cầu:

```text
docker compose --env-file ports.template up -d --build --wait
docker compose --env-file ports.template ps
uv run lab28 topics
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed --via-gateway
uv run lab28 inspect
uv run lab28 ready
```

### Kết quả mong đợi

- `docker compose ps`: các thành phần đang `running`/`healthy`;
- `lab28 topics`: các topic được `created` hoặc `exists`;
- `lab28 index`: có `points_upserted > 0`;
- `lab28 release`: có MLflow version và alias `champion`;
- `lab28 seed`: documents/feedback được `accepted`, không có `rejected`;
- `lab28 ready`: `ready` hoặc `degraded`; `not_ready` phải được điều tra.

### Các trang để quan sát

| UI | URL mặc định | Dùng để chứng minh |
|---|---|---|
| Cổng truy cập | <http://localhost:8080/health> | IP08 chuyển tiếp yêu cầu |
| Tài liệu API | <http://localhost:8000/docs> | Các yêu cầu HTTP hỗ trợ |
| Grafana | <http://localhost:3000> | Các số liệu chính của IP09 |
| Prometheus | <http://localhost:9090/targets> | Các thành phần đang gửi số liệu |
| Jaeger | <http://localhost:16686> | Một yêu cầu có cùng mã theo dõi từ đầu đến cuối |
| MLflow | <http://localhost:5000> | Phiên bản mô hình đang được chọn |
| Qdrant | <http://localhost:6333/dashboard> | Dữ liệu đã được lập chỉ mục |

Hệ thống cơ bản có thể báo LLM `degraded` nếu chưa nối vLLM thật; đây là trạng
thái đã dự kiến, không phải lý do để làm giả máy chủ vLLM.

## Bước 8 — Chạy toàn bộ luồng dữ liệu và máy học

Trên máy đủ mạnh hoặc môi trường do giảng viên cung cấp:

```text
docker compose --env-file ports.template --profile full up -d --build --wait
uv run lab28 seed --via-gateway
uv run pytest integration-tests/test_j1_golden_path.py -q
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
```

Mở Airflow tại <http://localhost:8082>, tìm DAG `lab28_ingestion_pipeline` và đối
chiếu nhật ký từng bước với Delta, Feast, Qdrant và MLflow.

Sau khi hai luồng đầu đạt, chạy toàn bộ phần không cần GPU:

```text
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

### Kết quả mong đợi

- J1: dữ liệu đi qua API → Kafka → Airflow → Delta → Feast/Qdrant → trả kết quả;
- J2: gửi lại cùng một lô dữ liệu không làm tăng số bản ghi;
- J3: chọn bản mới rồi quay lại đúng phiên bản trước;
- J4: một thành phần không bắt buộc bị lỗi, hệ thống báo `degraded`, rồi khôi phục;
- J5: mã theo dõi và số liệu giám sát được giữ xuyên suốt luồng;
- 3 kiểm thử cuối kiểm tra giới hạn lưu lượng, Prometheus và độ phủ theo dõi.

## Bước 9 — Nối vLLM thật khi có GPU

Kaggle hoặc máy GPU dùng chung được phép và giải quyết giới hạn phần cứng của lớp.
Làm theo [`KAGGLE_GPU_EXTENSION.md`](KAGGLE_GPU_EXTENSION.md), sau đó cấu hình
Compose bằng URL và mã mô hình do giảng viên cấp. Không đưa URL tạm hoặc token vào
Git.

Kiểm tra phải chứng minh được:

- `/version` là vLLM thật;
- `/v1/models` có model ID đã cấu hình;
- máy chủ có số liệu giám sát bắt đầu bằng `vllm:`;
- yêu cầu từ hệ thống trả về mã theo dõi, tên mô hình và phiên bản để đối chiếu.

Một máy chủ chỉ bắt chước OpenAI API nhưng không chứng minh được đó là vLLM thật
thì **không đạt IP07**.

## Bước 10 — Thu bằng chứng và luyện trình bày

```text
uv run lab28 evidence
uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

Theo [`docs/demo-runbook.md`](docs/demo-runbook.md), cá nhân hoặc nhóm trình bày:

### Danh sách kiểm tra demo (Demo checklist)

- [ ] Sơ đồ kiến trúc, người phụ trách và 10 điểm kết nối.
- [ ] Luồng chạy đúng có mã lần chạy, mã theo dõi, phiên bản Delta và MLflow.
- [ ] Kafka gửi lại dữ liệu nhưng Delta không có bản ghi trùng.
- [ ] Một sự cố có: dự đoán dấu hiệu → quan sát → khôi phục → chứng minh không mất dữ liệu.
- [ ] Các số liệu chính trên Grafana và một luồng theo dõi Jaeger xuyên hệ thống.
- [ ] MLflow chọn phiên bản mới rồi quay lại phiên bản trước mà không sửa mã.
- [ ] Giải thích được `ready`, `degraded` và `not_ready`.
- [ ] Tệp K8s/GitOps hợp lệ và giải thích được cách triển khai, quay lại bản trước.
- [ ] Người làm cá nhân hoặc từng thành viên nhóm giải thích được lựa chọn kỹ thuật
      của phần mình phụ trách.
- [ ] Không có mật khẩu, token, cơ sở dữ liệu tạm, bộ nhớ đệm hoặc trọng số mô
      hình trong phần mã gửi lên Git.

File nộp và câu hỏi reflection được liệt kê trong
[`SUBMISSION.md`](SUBMISSION.md).

### Bài nộp của nhánh này

| Mục trong `SUBMISSION.md` | Nằm ở |
|---|---|
| 1–2. Integration report + 10 file evidence | [`evidence/`](evidence/) |
| 3. Sơ đồ kiến trúc và người phụ trách | [`docs/images/lab28-architecture-overview.png`](docs/images/lab28-architecture-overview.png), [`docs/team-role-cards.md`](docs/team-role-cards.md) |
| 4–7. Happy path, sự cố/khôi phục, load profile, K8s/GitOps | [`docs/evidence-pack.md`](docs/evidence-pack.md) |
| 8. Trade-off, production gap, đóng góp | [`ANSWERS.md`](ANSWERS.md) |

## Xử lý lỗi (Troubleshooting)

| Triệu chứng | Nguyên nhân thường gặp | Cách kiểm tra/sửa |
|---|---|---|
| `uv: command not found` | `uv` chưa vào PATH | mở terminal mới, chạy lại installer chính thức rồi `uv --version` |
| Python sai version | dùng interpreter hệ thống | chạy lại `uv sync --python 3.11 ...` |
| Kết quả đầu không đúng 4 lỗi | chạy nhầm `pytest` hoặc kho mã cũ | dùng đúng `uv run pytest starter-tests -q`, rồi `git pull` |
| Kiểm thử riêng đạt nhưng toàn bộ kiểm thử lỗi | mã mới chỉ xử lý một trường hợp | đọc yêu cầu và tệp hệ thống được nêu trong phần tương ứng |
| Docker không hoạt động | Docker Desktop/Engine chưa chạy | mở Docker, đợi `docker info` thành công, chạy lại `preflight` |
| `port is already allocated` | cổng host đang được dùng | đổi giá trị port trong file override và dùng nó với `--env-file` |
| Thành phần báo `unhealthy` | thành phần phụ thuộc chưa sẵn sàng hoặc thiếu RAM | chạy `docker compose --env-file ports.template logs <ten-thanh-phan>`; sửa lỗi xuất hiện đầu tiên |
| API chạy nhưng `/ready` lỗi | tiến trình còn sống nhưng chưa sẵn sàng nhận yêu cầu | chạy `uv run lab28 ready`, tìm thành phần `not_ready` |
| Airflow không thấy DAG | import error/mount sai | xem Airflow UI và `docker compose ... logs airflow` |
| Delta có bản ghi trùng khi gửi lại | chọn sai khóa hoặc sai thứ tự | chạy riêng `tests/test_delta_merge_idempotency.py` |
| Feast trả `NOT_FOUND` | chưa có dữ liệu | xác nhận Airflow/Spark đã chạy và `asker_id` khớp |
| Qdrant có 0 điểm dữ liệu | chưa lập chỉ mục | chạy `uv run lab28 index --source file` trước khi trình bày |
| MLflow chưa có phiên bản được chọn | chưa chạy bước phát hành | chạy `uv run lab28 release` và kiểm tra trang MLflow |
| vLLM hết thời gian chờ | máy GPU đã tắt hoặc sai địa chỉ | kiểm tra `/version`, `/v1/models`; hệ thống cơ bản vẫn được phép ở trạng thái `degraded` |
| Mã theo dõi bị đứt ở Kafka | thiếu `traceparent` | chạy kiểm thử Phần A, đối chiếu hai phía cùng mã theo dõi |
| Máy yếu hoặc treo khi chạy toàn bộ | không đủ RAM/CPU | dừng hệ thống và chuyển phần này sang máy đủ mạnh hoặc môi trường do giảng viên cung cấp |

## Dọn môi trường

Dừng container nhưng giữ dữ liệu để lần sau chạy nhanh:

```text
docker compose --env-file ports.template --profile full down --remove-orphans
```

Chỉ khi muốn xóa cả dữ liệu tạm và vùng lưu trữ Docker:

```text
uv run lab28 reset --yes
```

Không dùng lệnh xóa trong phần trình bày khôi phục vì nó làm mất trạng thái trước
sự cố.

## Quy tắc quan trọng

1. Không sửa test để biến đỏ thành xanh.
2. Không làm giả vLLM, mã theo dõi, số liệu giám sát hoặc bằng chứng.
3. Không đưa mật khẩu, token, dữ liệu tạm, cơ sở dữ liệu, bộ nhớ đệm hay trọng số
   mô hình lên Git.
4. Dòng xử lý báo `SUCCESS` chưa đủ; phải chứng minh được dữ liệu, phiên bản, mã
   theo dõi và khả năng khôi phục.
5. Mỗi lệnh dùng khi trình bày phải có kết quả hoặc màn hình mà người làm giải
   thích được.
