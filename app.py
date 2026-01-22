#!/usr/bin/env python3
import argparse
import os
import sys
import time
from pathlib import Path
from typing import List, Optional
import requests

from cvat_sdk.api_client import Configuration, ApiClient, exceptions


def parse_task_ids(raw: str) -> List[int]:
    # Accept: "1,2,3" or "1 2 3" or mixed
    tokens = raw.replace(",", " ").split()
    ids = []
    for t in tokens:
        if not t.strip():
            continue
        ids.append(int(t))
    return ids


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def resolve_download_url(server: str, result_url: str) -> str:
    # CVAT may return absolute or relative URL
    if result_url.startswith("http://") or result_url.startswith("https://"):
        return result_url
    return server.rstrip("/") + "/" + result_url.lstrip("/")


def wait_for_request_finished(api_client: ApiClient, rq_id: str, poll_seconds: float, timeout_seconds: int) -> str:
    """
    Poll /api/requests/{id} until status is Finished/Failed and return result_url.
    Request model includes status/message/result_url. :contentReference[oaicite:2]{index=2}
    """
    start = time.time()
    last_status = None

    while True:
        (rq, _resp) = api_client.requests_api.retrieve(rq_id)

        status = str(rq.status)
        if status != last_status:
            print(f"  request {rq_id} status: {status} (progress={rq.progress})")
            last_status = status

        # status values are typically "queued", "started"/"in_progress", "finished", "failed"
        if status.lower() == "finished":
            if not rq.result_url:
                raise RuntimeError(f"Request {rq_id} finished but result_url is empty")
            return rq.result_url

        if status.lower() == "failed":
            raise RuntimeError(f"Request {rq_id} failed: {rq.message}")

        if time.time() - start > timeout_seconds:
            raise TimeoutError(f"Timeout waiting for request {rq_id} (last status={status})")

        time.sleep(poll_seconds)


def download_file(url: str, out_path: Path, username: str, password: str) -> None:
    # Basic auth works if your CVAT is configured that way.
    # If your instance uses token/session-only auth, tell me and I’ll adapt.
    with requests.get(url, auth=(username, password), stream=True) as r:
        r.raise_for_status()
        tmp_path = out_path.with_suffix(out_path.suffix + ".part")
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        tmp_path.replace(out_path)


def main():
    ap = argparse.ArgumentParser(description="Batch export/download CVAT task annotations via CVAT SDK")
    ap.add_argument("--server", required=True, help="e.g. https://cvat2.point-ai.com")
    ap.add_argument("--username", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--format", default="CVAT for video 1.1",
                    help="Export format name. (Same as your CLI format.) :contentReference[oaicite:3]{index=3}")
    ap.add_argument("--outdir", required=True, help="Directory to save downloaded zips")
    ap.add_argument("--task-ids", required=True,
                    help='Task IDs like "597,599,602" or "597 599 602"')
    ap.add_argument("--save-images", action="store_true",
                    help="Include images in export (default: annotations only). :contentReference[oaicite:4]{index=4}")
    ap.add_argument("--poll-seconds", type=float, default=2.0)
    ap.add_argument("--timeout-seconds", type=int, default=60 * 30)  # 30 minutes per task
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing output files")

    args = ap.parse_args()

    task_ids = parse_task_ids(args.task_ids)
    outdir = Path(args.outdir)
    ensure_dir(outdir)

    config = Configuration(
        host=args.server,
        username=args.username,
        password=args.password,
    )

    # Using low-level SDK APIs: TasksApi + RequestsApi. :contentReference[oaicite:5]{index=5}
    with ApiClient(config) as api_client:
        for task_id in task_ids:
            print(f"\nTask {task_id}: start export ({args.format})")

            out_file = outdir / f"task_{task_id}.zip"
            if out_file.exists() and not args.overwrite:
                print(f"  skip (exists): {out_file}")
                continue

            try:
                # Start export → returns rq_id :contentReference[oaicite:6]{index=6}
                (rqid_model, _resp) = api_client.tasks_api.create_dataset_export(
                    args.format,
                    task_id,
                    save_images=bool(args.save_images),
                    # filename=...  # optional server-side name
                    # location=...  # optional cloud_storage saving; omit for default
                )
                rq_id = rqid_model.rq_id

                print(f"  export queued, rq_id={rq_id}")
                result_url = wait_for_request_finished(
                    api_client, rq_id, poll_seconds=args.poll_seconds, timeout_seconds=args.timeout_seconds
                )

                download_url = resolve_download_url(args.server, result_url)
                print(f"  downloading: {download_url}")
                download_file(download_url, out_file, args.username, args.password)
                print(f"  saved: {out_file}")

            except exceptions.ApiException as e:
                print(f"  API error for task {task_id}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"  error for task {task_id}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
