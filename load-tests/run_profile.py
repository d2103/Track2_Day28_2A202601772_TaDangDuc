"""Cross-platform HTTP load probe using only the Python standard library.

Two profiles matter for this lab and they measure different things.

``/ready`` is the baseline: it fans out to every dependency probe, so it shows
gateway and readiness overhead without any model in the path. ``/api/v1/ask``
is the serving path the SLO is actually written against — feature lookup,
retrieval, the LLM call and the guardrails — so a profile that only ever hits
``/ready`` reports a latency the users never experience. Pass ``--path`` and
``--profile ask`` to measure it.
"""

from __future__ import annotations

import argparse
import json
import math
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

#: Representative questions over the bundled corpus, so retrieval is not served
#: from one cached embedding for every request in the run.
ASK_QUESTIONS = [
    "Nền tảng dữ liệu của lab này gồm những thành phần nào?",
    "Delta Lake giúp gì cho việc chống ghi trùng dữ liệu?",
    "Feast phục vụ đặc trưng trực tuyến như thế nào?",
    "Qdrant lưu vector tài liệu ra sao?",
    "MLflow quản lý phiên bản mô hình thế nào?",
]


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(quantile * len(ordered)) - 1)]


def request(url: str, path: str, body: bytes | None) -> tuple[float, int]:
    """One timed call. A failure is a status, not an exception.

    A load profile that stops at the first refused connection measures nothing;
    ``0`` for a transport error and the real code for an HTTP error both belong
    in the status histogram next to the successes.
    """
    target = f"{url.rstrip('/')}{path}"
    headers = {"content-type": "application/json"} if body is not None else {}
    call = urllib.request.Request(target, data=body, headers=headers)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(call, timeout=30) as response:
            status = response.status
    except urllib.error.HTTPError as error:  # 429 from the gateway is a result.
        status = error.code
    except Exception:
        status = 0
    return (time.perf_counter() - started) * 1000, status


def build_payload(profile: str, index: int, asker_id: str) -> bytes | None:
    """The request body for one call, or ``None`` for a GET profile."""
    if profile != "ask":
        return None
    return json.dumps(
        {
            "asker_id": asker_id,
            "question": ASK_QUESTIONS[index % len(ASK_QUESTIONS)],
            "locale": "vi",
            "top_k": 3,
        }
    ).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument(
        "--profile",
        choices=["ready", "ask"],
        default="ready",
        help="ready = dependency probe baseline; ask = the serving path under SLO.",
    )
    parser.add_argument(
        "--path",
        default=None,
        help="Override the route; defaults to the one implied by --profile.",
    )
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--asker-id",
        default="load-profile",
        help="Entity used for the ask profile's feature lookup.",
    )
    args = parser.parse_args()

    path = args.path or ("/api/v1/ask" if args.profile == "ask" else "/ready")
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(
            pool.map(
                lambda index: request(
                    args.url, path, build_payload(args.profile, index, args.asker_id)
                ),
                range(args.requests),
            )
        )
    wall_seconds = time.perf_counter() - started

    durations = [duration for duration, _ in results]
    statuses: dict[str, int] = {}
    for _, status in results:
        statuses[str(status)] = statuses.get(str(status), 0) + 1
    successes = [
        duration for duration, status in results if 200 <= status < 300
    ]
    print(
        json.dumps(
            {
                "url": f"{args.url.rstrip('/')}{path}",
                "profile": args.profile,
                "requests": args.requests,
                "workers": args.workers,
                "wall_seconds": round(wall_seconds, 3),
                "throughput_rps": round(args.requests / wall_seconds, 2),
                "status_counts": statuses,
                "success_ratio": round(len(successes) / len(results), 4),
                "latency_ms": {
                    "p50": percentile(durations, 0.50),
                    "p95": percentile(durations, 0.95),
                    "p99": percentile(durations, 0.99),
                },
                # Rate-limited and failed calls return fast and would otherwise
                # flatter the percentiles above.
                "successful_latency_ms": {
                    "p50": percentile(successes, 0.50) if successes else None,
                    "p95": percentile(successes, 0.95) if successes else None,
                    "p99": percentile(successes, 0.99) if successes else None,
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
