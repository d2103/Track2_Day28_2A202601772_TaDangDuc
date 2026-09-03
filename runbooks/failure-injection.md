# Failure injection & recovery

Chỉ thao tác service thuộc project `lab28-platform`; ghi timestamp và state trước/sau.

| Scenario | Inject | Expected | Recovery proof |
|---|---|---|---|
| Feast down | `docker compose stop feast` | degraded reason visible | start; lookup present |
| Qdrant down | `docker compose stop qdrant` | not_ready/protected request | start; same count |
| Kafka down | `docker compose stop kafka` | ingestion 503 | start; consume once |
| vLLM down | stop endpoint | degraded/503 per policy | restore; identity passes |
| Airflow down | `docker compose stop airflow` | ingestion vẫn 202, không có gì được xử lý | start; chạy DAG drain; row Delta tăng đúng số đã nhận |

Không dùng `down -v` vì sẽ xóa state. Chỉ replay DLQ sau khi sửa nguyên nhân.

Đọc **component list** của `/ready`, không chỉ đọc chữ status ở đầu: khi một
phụ thuộc tùy chọn khác đã hỏng sẵn, status tổng vẫn là `degraded` trước và sau
khi inject, và chỉ component list cho biết cái gì vừa đổi.

Hai kịch bản đã chạy và ghi lại nằm ở
[`docs/evidence-pack.md`](../docs/evidence-pack.md) mục 3.
