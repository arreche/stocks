import os
from huggingface_hub import HfApi

api = HfApi(token=os.getenv("HF_TOKEN"))
api.upload_file(
    path_or_fileobj="stocks.parquet",
    path_in_repo="stocks.parquet",
    repo_id="Arrechenash/stocks",
    repo_type="dataset",
)
