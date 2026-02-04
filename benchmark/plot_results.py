#!/usr/bin/env python3
"""Generate SVG chart from daily benchmark results and upload to S3."""

import csv
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from botocore.config import Config
from botocore.exceptions import ClientError

RESULTS_BUCKET = "hippius-benchmarks"
CSV_KEY = "daily.csv"
SVG_KEY = "daily.svg"


SCENARIOS = [
    "put_1mb", "get_1mb",
    "put_100mb", "get_100mb",
    "put_1gb", "get_1gb",
]


def load_data(csv_path: Path, days: int = 30) -> dict:
    cutoff = datetime.now() - timedelta(days=days)
    data = {s: [] for s in SCENARIOS}

    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            date = datetime.strptime(row["date"], "%Y-%m-%d")
            if date < cutoff:
                continue
            scenario = row["scenario"]
            if scenario in data and row["status"] == "ok":
                data[scenario].append((date, float(row["throughput_mbps"])))

    return data


def plot_chart(data: dict, output_path: Path):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)

    put_ax, get_ax = axes

    for scenario, points in data.items():
        if not points:
            continue
        points.sort(key=lambda x: x[0])
        dates, values = zip(*points)
        label = scenario.split("_", 1)[1].upper()
        ax = put_ax if scenario.startswith("put") else get_ax
        ax.plot(dates, values, marker="o", markersize=3, label=label)

    for ax, title in [(put_ax, "Upload (PUT)"), (get_ax, "Download (GET)")]:
        ax.set_xlabel("Date")
        ax.set_ylabel("Throughput (MB/s)")
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))

    fig.suptitle("Hippius S3 Daily Benchmark (last 30 days)", fontsize=13)
    plt.tight_layout()
    plt.savefig(output_path, format="svg")
    plt.close()
    print(f"Chart saved to {output_path}")


def main():
    endpoint = os.environ.get("HIPPIUS_ENDPOINT") or os.environ.get("S3_ENDPOINT_URL", "https://s3.hippius.com")
    access_key = os.environ.get("AWS_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        print("Missing AWS_ACCESS_KEY/AWS_ACCESS_KEY_ID or AWS_SECRET_KEY/AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="decentralized",
        config=Config(s3={"addressing_style": "path"}),
    )

    # Download CSV from S3
    tmp = Path(tempfile.gettempdir())
    csv_path = tmp / "daily_bench.csv"
    svg_path = tmp / "daily_bench.svg"

    try:
        client.download_file(RESULTS_BUCKET, CSV_KEY, str(csv_path))
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"No data file in S3: s3://{RESULTS_BUCKET}/{CSV_KEY}")
            return
        raise

    # Generate chart
    data = load_data(csv_path)
    plot_chart(data, svg_path)

    # Upload SVG to S3
    client.upload_file(
        str(svg_path),
        RESULTS_BUCKET,
        SVG_KEY,
        ExtraArgs={"ContentType": "image/svg+xml"},
    )
    print(f"Chart uploaded to s3://{RESULTS_BUCKET}/{SVG_KEY}")


if __name__ == "__main__":
    main()
