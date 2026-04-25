import os
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXAMPLES_DIR = os.path.join(ROOT, "examples")
PARQUET_PATH = os.path.join(EXAMPLES_DIR, "data_seek.parquet")


def main() -> None:
    os.makedirs(EXAMPLES_DIR, exist_ok=True)

    num_rows = 1000
    table = pa.table(
        {
            "id": pa.array(list(range(num_rows)), type=pa.int64()),
            "grp": pa.array([f"g{i % 10}" for i in range(num_rows)]),
            "value": pa.array([float(i) / 10.0 for i in range(num_rows)], type=pa.float64()),
        }
    )
    pq.write_table(table, PARQUET_PATH, row_group_size=128)

    fixtures = [
        ("parquet_seek_deflated.zip", zipfile.ZIP_DEFLATED),
        ("parquet_seek_stored.zip", zipfile.ZIP_STORED),
    ]
    for filename, compression in fixtures:
        archive_path = os.path.join(EXAMPLES_DIR, filename)
        with zipfile.ZipFile(archive_path, "w", compression=compression) as zf:
            zf.write(PARQUET_PATH, arcname="data.parquet")
        print(f"wrote {archive_path}")


if __name__ == "__main__":
    main()
