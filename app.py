import streamlit as st
import time
from pathlib import Path
import requests

from cvat_sdk.api_client import Configuration, ApiClient, exceptions


# --------------------------
# Helpers
# --------------------------
def parse_task_ids(raw: str):
    return [int(x) for x in raw.replace(",", " ").split() if x.strip()]


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def resolve_download_url(server: str, result_url: str) -> str:
    if result_url.startswith("http"):
        return result_url
    return server.rstrip("/") + "/" + result_url.lstrip("/")


def wait_for_request(api_client, rq_id, poll=2.0):
    while True:
        rq, _ = api_client.requests_api.retrieve(rq_id)
        if rq.status.lower() == "finished":
            return rq.result_url
        if rq.status.lower() == "failed":
            raise RuntimeError(rq.message)
        time.sleep(poll)


def download(url, out_path, auth):
    with requests.get(url, auth=auth, stream=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)


# --------------------------
# Streamlit UI
# --------------------------
st.set_page_config(layout="wide")
st.title("CVAT – Batch Download Tasks (SDK)")

server = st.text_input("CVAT server", "https://cvat2.point-ai.com")

col1, col2 = st.columns(2)
with col1:
    username = st.text_input("Username", "Point_AI")
with col2:
    password = st.text_input("Password", type="password")

export_format = st.text_input("Export format", "CVAT for video 1.1")
out_dir = st.text_input(
    "Output directory",
    "/Volumes/Point.AI Data/Mafat/Projects/RE_ID/Results/Batch_2_Bat_Hefer/CVAT_XML",
)

task_ids_raw = st.text_area(
    "Task IDs (comma or space separated)",
    "597 599 602 685",
    height=120,
)

start = st.button("Start download")

if start:
    if not password:
        st.error("Password required")
        st.stop()

    task_ids = parse_task_ids(task_ids_raw)
    out_dir = Path(out_dir)
    ensure_dir(out_dir)

    config = Configuration(
        host=server,
        username=username,
        password=password,
    )

    log = st.empty()
    progress = st.progress(0.0)
    logs = []

    with ApiClient(config) as api_client:
        for i, task_id in enumerate(task_ids, 1):
            try:
                logs.append(f"▶ Task {task_id}: exporting…")
                log.code("\n".join(logs))

                rq, _ = api_client.tasks_api.create_dataset_export(
                    export_format,
                    task_id,
                    save_images=False,
                )

                result_url = wait_for_request(api_client, rq.rq_id)
                dl_url = resolve_download_url(server, result_url)

                out_file = out_dir / f"task_{task_id}.zip"
