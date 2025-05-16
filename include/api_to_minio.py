from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import pandas as pd
import s3fs
import logging
from io import BytesIO
from typing import Optional, Dict, Any
import re
import pyarrow.parquet as pq
import pyarrow as pa
import requests
import json
from pathlib import Path

def sanitize_minio_path(path: str) -> str:
    """
    Sanitizes a path for use in MinIO, replacing invalid characters.
    """
    return re.sub(r"[^a-zA-Z0-9\-_\./]", "_", path)

class ApiToMinioParquetOperator(BaseOperator):
    """
    Loads data from an API endpoint to MinIO as Parquet file(s) with support for different update types
    and batch processing.

    Args:
        api_endpoint (str): The base URL of the API endpoint.
        api_auth (Optional[Dict]): Authentication details (e.g., {'api_key': 'key'}, {'token': 'token'}, {'username': 'user', 'password': 'pass'}).
        api_headers (Optional[Dict]): Custom headers for API requests.
        api_params (Optional[Dict]): Query parameters for the API request.
        source_key (str): Key in the API response JSON containing the data (e.g., 'results').
        minio_endpoint_url (str): MinIO endpoint URL.
        minio_access_key (str): MinIO access key.
        minio_secret_key (str): MinIO secret key.
        minio_bucket (str): MinIO bucket name.
        goo (str): Goo identifier for path.
        iss (str): ISS identifier for path.
        update_type (str): Type of update ('insert', 'insert_update', 'insert_update_delete').
        full_refresh (bool): Whether to perform a full refresh for 'insert_update' case.
        watermark_col (Optional[str]): Field used for incremental updates (e.g., 'updated_at').
        table_name (Optional[str]): Custom table name for output file.
        partition_cols (Optional[list]): Columns to partition data by in MinIO.
        use_threads (bool): Whether to use threads for S3 transfer.
        batch_size (Optional[int]): Number of rows per batch for processing.
        pagination_type (Optional[str]): Type of pagination ('page', 'offset', 'token', None).
        pagination_param (Optional[str]): Parameter name for pagination (e.g., 'page', 'offset').
        pagination_start (Optional[int]): Starting value for pagination (default: 1 for page, 0 for offset).
        pagination_limit (Optional[int]): Number of records per page (if applicable).
    """

    @apply_defaults
    def __init__(
        self,
        minio_endpoint_url: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_bucket: str,
        goo: str,
        iss: str,
        update_type: str,
        api_endpoint: str,
        api_auth: Optional[Dict] = None,
        api_headers: Optional[Dict] = None,
        api_params: Optional[Dict] = None,
        source_key: str = "results",
        full_refresh: bool = False,
        watermark_col: Optional[str] = None,
        table_name: Optional[str] = None,
        partition_cols: Optional[list] = None,
        use_threads: bool = True,
        batch_size: Optional[int] = None,
        pagination_type: Optional[str] = None,
        pagination_param: Optional[str] = None,
        pagination_start: Optional[int] = None,
        pagination_limit: Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.api_endpoint = api_endpoint
        self.api_auth = api_auth or {}
        self.api_headers = api_headers or {}
        self.api_params = api_params or {}
        self.source_key = source_key
        self.minio_endpoint_url = minio_endpoint_url
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.goo = goo
        self.iss = iss
        self.update_type = update_type.lower()
        self.full_refresh = full_refresh
        self.watermark_col = watermark_col
        self.table_name = table_name
        self.partition_cols = partition_cols
        self.use_threads = use_threads
        self.batch_size = batch_size
        self.pagination_type = pagination_type
        self.pagination_param = pagination_param
        self.pagination_start = pagination_start if pagination_start is not None else (1 if pagination_type == "page" else 0)
        self.pagination_limit = pagination_limit

        # Validate update_type
        valid_update_types = ["insert", "insert_update", "insert_update_delete"]
        if self.update_type not in valid_update_types:
            raise ValueError(f"Invalid update_type: {self.update_type}. Must be one of {valid_update_types}")

        # Validate pagination
        if self.pagination_type and self.pagination_type not in ["page", "offset", "token"]:
            raise ValueError(f"Invalid pagination_type: {self.pagination_type}. Must be one of ['page', 'offset', 'token']")
        if self.pagination_type in ["page", "offset"] and not self.pagination_param:
            raise ValueError(f"pagination_param is required for pagination_type '{self.pagination_type}'")

    def _get_s3fs(self):
        """Initialize and return s3fs filesystem."""
        return s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": self.minio_endpoint_url},
            key=self.minio_access_key,
            secret=self.minio_secret_key,
        )

    def _get_max_watermark(self, fs, path_prefix: str, find_latest_day: bool = False) -> Optional[Any]:
        """
        Read the maximum watermark value from the latest Parquet file in the given path prefix.
        """
        try:
            if find_latest_day:
                day_dirs = fs.glob(f"{path_prefix}/*/*/*")
                if not day_dirs:
                    logging.info(f"No day directories found in {path_prefix}. Assuming first-time load.")
                    return None

                parquet_files = []
                for day_dir in day_dirs:
                    files = fs.glob(f"{day_dir}/*.parquet")
                    parquet_files.extend(files)

                if not parquet_files:
                    logging.info(f"No Parquet files found in {path_prefix}. Assuming first-time load.")
                    return None

                latest_file = sorted(parquet_files)[-1]
                logging.info(f"Found latest Parquet file: {latest_file}")
            else:
                parquet_files = fs.glob(f"{path_prefix}/*.parquet")
                if not parquet_files:
                    logging.info(f"No Parquet files found in {path_prefix}. Assuming first-time load.")
                    return None
                latest_file = sorted(parquet_files)[-1]
                logging.info(f"Reading watermark from: {latest_file}")

            with fs.open(latest_file, "rb") as f:
                df = pd.read_parquet(f, columns=[self.watermark_col])
                if self.watermark_col not in df.columns:
                    logging.error(f"Watermark column {self.watermark_col} not found in {latest_file}")
                    return None
                max_value = df[self.watermark_col].max()
                logging.info(f"Max watermark value: {max_value}")
                return max_value
        except Exception as e:
            logging.error(f"Error reading watermark from {path_prefix}: {e}")
            return None

    def _fetch_api_data(self, params: Dict, pagination_value: Optional[Any] = None) -> list:
        """
        Fetch data from the API with the given parameters and pagination value.
        """
        try:
            request_params = params.copy()
            if pagination_value is not None and self.pagination_param:
                request_params[self.pagination_param] = pagination_value

            # Handle authentication
            auth = None
            if "username" in self.api_auth and "password" in self.api_auth:
                auth = (self.api_auth["username"], self.api_auth["password"])
            elif "api_key" in self.api_auth:
                request_params["api_key"] = self.api_auth["api_key"]
            elif "token" in self.api_auth:
                self.api_headers["Authorization"] = f"Bearer {self.api_auth['token']}"

            response = requests.get(
                self.api_endpoint,
                params=request_params,
                headers=self.api_headers,
                auth=auth,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            # Extract the data from the specified source_key
            results = data.get(self.source_key, data) if self.source_key else data
            if not isinstance(results, list):
                results = [results]

            # Handle pagination metadata for 'token' type
            next_pagination_value = None
            if self.pagination_type == "token":
                next_pagination_value = data.get("next_token") or data.get("next_page")

            return results, next_pagination_value
        except Exception as e:
            logging.error(f"Error fetching data from API: {e}")
            raise

    def _construct_api_params(self, max_watermark: Optional[Any] = None) -> Dict:
        """
        Construct API query parameters based on the watermark.
        """
        params = self.api_params.copy()
        if max_watermark is not None and self.watermark_col:
            params["since"] = max_watermark  # Assumes API supports 'since' parameter for watermark
        return params

    def _write_to_minio(self, fs, df: pd.DataFrame, output_path: str, batch_index: Optional[int] = None):
        """
        Write DataFrame to MinIO as Parquet, optionally appending batch index to filename.
        """
        try:
            df = df.copy()
            if self.partition_cols:
                for col in self.partition_cols:
                    df[col] = df[col].apply(sanitize_minio_path)

                if batch_index is not None:
                    output_path = output_path.replace(".parquet", f"_batch_{batch_index}.parquet")

                df.to_parquet(
                    f"s3://{output_path}",
                    engine="pyarrow",
                    partition_cols=self.partition_cols,
                    storage_options={
                        "client_kwargs": {"endpoint_url": self.minio_endpoint_url},
                        "key": self.minio_access_key,
                        "secret": self.minio_secret_key,
                    },
                )
            else:
                if batch_index is not None:
                    output_path = output_path.replace(".parquet", f"_batch_{batch_index}.parquet")

                df.to_parquet(
                    f"s3://{output_path}",
                    engine="pyarrow",
                    storage_options={
                        "client_kwargs": {"endpoint_url": self.minio_endpoint_url},
                        "key": self.minio_access_key,
                        "secret": self.minio_secret_key,
                    },
                )
            logging.info(f"Successfully wrote batch {batch_index if batch_index is not None else 'single'} to {output_path}")
        except Exception as e:
            logging.error(f"Error writing to MinIO at {output_path}: {e}")
            raise

    def execute(self, context):
        """
        Execute the operator with batch processing and pagination support.
        """
        fs = self._get_s3fs()

        # Determine output filename
        output_filename = self.table_name or "api_data"
        output_filename = f"{output_filename}.parquet"

        dt = datetime(2025, 3, 20)  # For testing purposes
        date_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"

        if self.update_type == "insert":
            if not self.watermark_col:
                raise ValueError("watermark_col is required for 'insert' update_type")

            path_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data"
            max_watermark = self._get_max_watermark(fs, path_prefix, find_latest_day=True)

            params = self._construct_api_params(max_watermark)
            logging.info(f"Fetching API data with params: {params}")

            try:
                all_data = []
                if self.pagination_type in ["page", "offset"]:
                    pagination_value = self.pagination_start
                    while True:
                        data, _ = self._fetch_api_data(params, pagination_value)
                        if not data:
                            break
                        all_data.extend(data)
                        pagination_value += 1 if self.pagination_type == "page" else self.pagination_limit
                        if self.pagination_limit and len(data) < self.pagination_limit:
                            break
                elif self.pagination_type == "token":
                    next_token = None
                    while True:
                        data, next_token = self._fetch_api_data(params, next_token)
                        if not data:
                            break
                        all_data.extend(data)
                        if not next_token:
                            break
                else:
                    data, _ = self._fetch_api_data(params)
                    all_data.extend(data)

                if not all_data:
                    logging.info("No data returned from API.")
                    return

                df = pd.DataFrame(all_data)
                logging.info(f"Fetched {len(df)} rows from API.")

                if self.batch_size:
                    batch_index = 0
                    for start in range(0, len(df), self.batch_size):
                        chunk = df.iloc[start:start + self.batch_size]
                        logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                        if not chunk.empty:
                            self._write_to_minio(fs, chunk, date_path, batch_index)
                            batch_index += 1
                    if batch_index == 0:
                        logging.info("No data to write to MinIO.")
                else:
                    if not df.empty:
                        self._write_to_minio(fs, df, date_path)
                    else:
                        logging.info("No data to write to MinIO.")
            except Exception as e:
                logging.error(f"Error processing API data: {e}")
                raise

        elif self.update_type == "insert_update":
            if self.full_refresh:
                logging.info("Performing full refresh for insert_update.")
                try:
                    all_data = []
                    if self.pagination_type in ["page", "offset"]:
                        pagination_value = self.pagination_start
                        while True:
                            data, _ = self._fetch_api_data(self.api_params, pagination_value)
                            if not data:
                                break
                            all_data.extend(data)
                            pagination_value += 1 if self.pagination_type == "page" else self.pagination_limit
                            if self.pagination_limit and len(data) < self.pagination_limit:
                                break
                    elif self.pagination_type == "token":
                        next_token = None
                        while True:
                            data, next_token = self._fetch_api_data(self.api_params, next_token)
                            if not data:
                                break
                            all_data.extend(data)
                            if not next_token:
                                break
                    else:
                        data, _ = self._fetch_api_data(self.api_params)
                        all_data.extend(data)

                    if not all_data:
                        logging.warning("No data returned from API.")
                        return

                    df = pd.DataFrame(all_data)
                    logging.info(f"Fetched {len(df)} rows from API.")

                    full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/full/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                    if self.batch_size:
                        batch_index = 0
                        for start in range(0, len(df), self.batch_size):
                            chunk = df.iloc[start:start + self.batch_size]
                            logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                            if not chunk.empty:
                                self._write_to_minio(fs, chunk, full_path, batch_index)
                                batch_index += 1
                        if batch_index == 0:
                            logging.warning("No data to write to MinIO.")
                            return
                    else:
                        if not df.empty:
                            self._write_to_minio(fs, df, full_path)
                        else:
                            logging.warning("No data to write to MinIO.")
                            return

                    incremental_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/incremental"
                    try:
                        if fs.exists(incremental_path):
                            logging.info(f"Deleting existing incremental directory: {incremental_path}")
                            fs.rm(incremental_path, recursive=True)
                            logging.info(f"Successfully deleted {incremental_path}")
                        else:
                            logging.info(f"No incremental directory found at {incremental_path}")
                    except Exception as e:
                        logging.error(f"Error deleting incremental directory {incremental_path}: {e}")
                        raise
                except Exception as e:
                    logging.error(f"Error processing API data: {e}")
                    raise
            else:
                if not self.watermark_col:
                    raise ValueError("watermark_col is required for 'insert_update' with full_refresh=False")

                full_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/full"
                incr_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/incremental"
                max_watermark = None

                full_watermark = self._get_max_watermark(fs, full_prefix)
                incr_watermark = self._get_max_watermark(fs, incr_prefix)

                if full_watermark is not None and incr_watermark is not None:
                    max_watermark = max(full_watermark, incr_watermark)
                elif full_watermark is not None:
                    max_watermark = full_watermark
                elif incr_watermark is not None:
                    max_watermark = incr_watermark
                else:
                    logging.info("No previous data found in full/ or incremental/. Loading full dataset.")

                params = self._construct_api_params(max_watermark)
                logging.info(f"Fetching API data with params: {params}")

                try:
                    all_data = []
                    if self.pagination_type in ["page", "offset"]:
                        pagination_value = self.pagination_start
                        while True:
                            data, _ = self._fetch_api_data(params, pagination_value)
                            if not data:
                                break
                            all_data.extend(data)
                            pagination_value += 1 if self.pagination_type == "page" else self.pagination_limit
                            if self.pagination_limit and len(data) < self.pagination_limit:
                                break
                    elif self.pagination_type == "token":
                        next_token = None
                        while True:
                            data, next_token = self._fetch_api_data(params, next_token)
                            if not data:
                                break
                            all_data.extend(data)
                            if not next_token:
                                break
                    else:
                        data, _ = self._fetch_api_data(params)
                        all_data.extend(data)

                    if not all_data:
                        logging.info("No new data returned from API.")
                        return

                    df = pd.DataFrame(all_data)
                    logging.info(f"Fetched {len(df)} rows from API.")

                    incr_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                    if self.batch_size:
                        batch_index = 0
                        for start in range(0, len(df), self.batch_size):
                            chunk = df.iloc[start:start + self.batch_size]
                            logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                            if not chunk.empty:
                                self._write_to_minio(fs, chunk, incr_path, batch_index)
                                batch_index += 1
                        if batch_index == 0:
                            logging.info("No data to write to MinIO.")
                    else:
                        if not df.empty:
                            self._write_to_minio(fs, df, incr_path)
                        else:
                            logging.info("No data to write to MinIO.")
                except Exception as e:
                    logging.error(f"Error processing API data: {e}")
                    raise

        elif self.update_type == "insert_update_delete":
            logging.info("Performing full overwrite for insert_update_delete.")
            try:
                all_data = []
                if self.pagination_type in ["page", "offset"]:
                    pagination_value = self.pagination_start
                    while True:
                        data, _ = self._fetch_api_data(self.api_params, pagination_value)
                        if not data:
                            break
                        all_data.extend(data)
                        pagination_value += 1 if self.pagination_type == "page" else self.pagination_limit
                        if self.pagination_limit and len(data) < self.pagination_limit:
                            break
                elif self.pagination_type == "token":
                    next_token = None
                    while True:
                        data, next_token = self._fetch_api_data(self.api_params, next_token)
                        if not data:
                            break
                        all_data.extend(data)
                        if not next_token:
                            break
                else:
                    data, _ = self._fetch_api_data(self.api_params)
                    all_data.extend(data)

                if not all_data:
                    logging.warning("No data returned from API.")
                    return

                df = pd.DataFrame(all_data)
                logging.info(f"Fetched {len(df)} rows from API.")

                full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/api_data/full/{output_filename}"
                if self.batch_size:
                    batch_index = 0
                    for start in range(0, len(df), self.batch_size):
                        chunk = df.iloc[start:start + self.batch_size]
                        logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                        if not chunk.empty:
                            self._write_to_minio(fs, chunk, full_path, batch_index)
                            batch_index += 1
                    if batch_index == 0:
                        logging.warning("No data to write to MinIO.")
                        return
                else:
                    if not df.empty:
                        self._write_to_minio(fs, df, full_path)
                    else:
                        logging.warning("No data to write to MinIO.")
                        return
            except Exception as e:
                logging.error(f"Error processing API data: {e}")
                raise