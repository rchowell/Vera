from __future__ import annotations

import os
import pathlib
from re import S
import time

import daft
from daft.functions.ai import embed_text


def mkdir() -> str:
    desktop = os.path.join(pathlib.Path("~").expanduser(), "Desktop")
    timestamp = str(int(time.time()))
    path = os.path.join(desktop, timestamp)
    pathlib.Path(path).mkdir(exist_ok=True, parents=True)
    return path


def my_workflow(name: str) -> dict:
    print(f"Hello, {name}!, starting a daft job")
    df = daft.from_pydict(
        {
            "text": [
                "Alice was beginning to get very tired of sitting by her sister on the bank.",
                "So she was considering in her own mind (as well as she could, for the hot day made her feel very sleepy and stupid),",
                "whether the pleasure of making a daisy-chain would be worth the trouble of getting up and picking the daisies,",
                "when suddenly a White Rabbit with pink eyes ran close by her.",
                "There was nothing so very remarkable in that;",
                "nor did Alice think it so very much out of the way to hear the Rabbit say to itself, 'Oh dear! Oh dear! I shall be late!'",
            ]
        }
    )

    for i in range(2):
        print(f"Newly Processing {i} of 2")
        time.sleep(1)

    results = {}
    for i in range(3):
        dest = mkdir()
        df_with_emb = df.with_column("embedding", embed_text(df["text"]))
        df_with_emb = df_with_emb.write_parquet(dest)
        results[f"run_{i + 1}"] = dest

    for i in range(2):
        print(f"Processing again {i} of 300")
        time.sleep(1)

    print(f"Thanks, {name}!, daft job completed")
    return {"results": results}
