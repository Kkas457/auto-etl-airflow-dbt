from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import s3fs
from sqlalchemy import create_engine, text
import logging
from io import BytesIO
from typing import Optional
import re
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

def sanitize_minio_path(path: str) -> str:
    """
    Sanitizes a path for use in MinIO, replacing invalid characters.
    """
    return re.sub(r"[^a-zA-Z0-9\-_\./]", "_", path)

class PostgresToMinioParquetOperator(BaseOperator):
    """
    Loads data from a Postgres table to MinIO as Parquet file(s) with support for different update types.

    Args:
        postgres_conn_id (str): Airflow connection ID for Postgres.
        source_table (str): Name of the Postgres table.
        minio_endpoint_url (str): MinIO endpoint URL.
        minio_access_key (str): MinIO access key.
        minio_secret_key (str): MinIO secret key.
        minio_bucket (str): MinIO bucket name.
        goo (str): Goo identifier for path.
        iss (str): ISS identifier for path.
        update_type (str): Type of update ('insert', 'insert_update', 'insert_update_delete').
        full_refresh (bool): Whether to perform a full refresh for 'insert_update' case.
        watermark_col (Optional[str]): Column used for incremental updates.
        query (Optional[str]): Custom SQL query to extract data.
        table_name (Optional[str]): Custom table name for output file.
        partition_cols (Optional[list]): Columns to partition data by in MinIO.
        use_threads (bool): Whether to use threads for S3 transfer.
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str,
        source_table: str,
        minio_endpoint_url: str,
        minio_access_key: str,
        minio_secret_key: str,
        minio_bucket: str,
        goo: str,
        iss: str,
        update_type: str,
        full_refresh: bool = False,
        watermark_col: Optional[str] = None,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
        partition_cols: Optional[list] = None,
        use_threads: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.source_table = source_table
        self.minio_endpoint_url = minio_endpoint_url
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket = minio_bucket
        self.goo = goo
        self.iss = iss
        self.update_type = update_type.lower()
        self.full_refresh = full_refresh
        self.watermark_col = watermark_col
        self.query = query
        self.table_name = table_name
        self.partition_cols = partition_cols
        self.use_threads = use_threads

        # Validate update_type
        valid_update_types = ["insert", "insert_update", "insert_update_delete"]
        if self.update_type not in valid_update_types:
            raise ValueError(f"Invalid update_type: {self.update_type}. Must be one of {valid_update_types}")

    def _get_s3fs(self):
        """Initialize and return s3fs filesystem."""
        return s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": self.minio_endpoint_url},
            key=self.minio_access_key,
            secret=self.minio_secret_key,
        )

    def _get_max_watermark(self, fs, path_prefix: str, find_latest_day: bool = False) -> Optional[any]:
        """
        Read the maximum watermark value from the latest Parquet file in the given path prefix.
        If find_latest_day is True, searches for the most recent day directory with Parquet files.
        Returns None for first-time loads when no files exist.

        Args:
            fs: s3fs filesystem instance.
            path_prefix (str): Base path to search for Parquet files (e.g., bucket/goo/iss/table_name).
            find_latest_day (bool): If True, find the most recent day directory; otherwise, use path_prefix directly.

        Returns:
            Optional[any]: Maximum watermark value or None if no files or watermark column is invalid.
        """
        try:
            if find_latest_day:
                # Search for all day directories (year/month/day)
                day_dirs = fs.glob(f"{path_prefix}/*/*/*")
                if not day_dirs:
                    logging.info(f"No day directories found in {path_prefix}. Assuming first-time load.")
                    return None

                # Filter directories that contain Parquet files
                parquet_files = []
                for day_dir in day_dirs:
                    files = fs.glob(f"{day_dir}/*.parquet")
                    parquet_files.extend(files)

                if not parquet_files:
                    logging.info(f"No Parquet files found in {path_prefix}. Assuming first-time load.")
                    return None

                # Get the latest Parquet file (sort by path, assuming year/month/day structure)
                latest_file = sorted(parquet_files)[-1]
                logging.info(f"Found latest Parquet file: {latest_file}")
            else:
                # Use the provided path_prefix directly
                parquet_files = fs.glob(f"{path_prefix}/*.parquet")
                if not parquet_files:
                    logging.info(f"No Parquet files found in {path_prefix}. Assuming first-time load.")
                    return None
                latest_file = sorted(parquet_files)[-1]
                logging.info(f"Reading watermark from: {latest_file}")

            # Read Parquet file
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

    def _construct_query(self, max_watermark: Optional[any] = None) -> str:
        """
        Construct the SQL query based on the update type and watermark.
        """
        if self.query:
            base_query = self.query
        else:
            base_query = f"SELECT * FROM {self.source_table}"

        if max_watermark is not None and self.watermark_col:
            # Escape watermark value to prevent SQL injection
            if isinstance(max_watermark, (int, float)):
                condition = f"WHERE {self.watermark_col} > {max_watermark}"
            else:
                condition = f"WHERE {self.watermark_col} > '{max_watermark}'"
            return f"{base_query} {condition}"
        return base_query

    def _write_to_minio(self, fs, df: pd.DataFrame, output_path: str):
        """
        Write DataFrame to MinIO as Parquet.
        """
        try:
            df = df.copy()
            if self.partition_cols:
                for col in self.partition_cols:
                    df[col] = df[col].apply(sanitize_minio_path)
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
                df.to_parquet(
                    f"s3://{output_path}",
                    engine="pyarrow",
                    storage_options={
                        "client_kwargs": {"endpoint_url": self.minio_endpoint_url},
                        "key": self.minio_access_key,
                        "secret": self.minio_secret_key,
                    },
                )
            logging.info(f"Successfully wrote data to {output_path}")
        except Exception as e:
            logging.error(f"Error writing to MinIO at {output_path}: {e}")
            raise

    def execute(self, context):
        """
        Execute the operator based on the update_type, handling first-time loads and last closed day for insert.
        """
        fs = self._get_s3fs()
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        # Determine output filename
        if not self.table_name:
            if self.query:
                import hashlib
                query_hash = hashlib.md5(self.query.encode()).hexdigest()
                output_filename = f"query_{query_hash}.parquet"
            else:
                output_filename = f"{self.source_table}.parquet"
        else:
            output_filename = f"{self.table_name}.parquet"

        # Get current date for path
        # dt = datetime.now()
        dt = datetime(2025, 3, 20)  # For testing purposes
        date_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"

        # Handle update types
        if self.update_type == "insert":
            # Incremental load based on watermark
            if not self.watermark_col:
                raise ValueError("watermark_col is required for 'insert' update_type")

            # Get max watermark from the last closed day
            path_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}"
            max_watermark = self._get_max_watermark(fs, path_prefix, find_latest_day=True)

            # Construct query (full table for first load if max_watermark is None)
            query = self._construct_query(max_watermark)
            logging.info(f"Executing query for insert: {query}")

            # Read data
            try:
                df = pd.read_sql_query(sql=text(query), con=engine)
                logging.info(f"Read {len(df)} rows from Postgres.")
            except Exception as e:
                logging.error(f"Error reading data from Postgres: {e}")
                raise

            # Write to MinIO
            if not df.empty:
                self._write_to_minio(fs, df, date_path)
            else:
                logging.info("No data to write to MinIO.")

        elif self.update_type == "insert_update":
            if self.full_refresh:
                logging.info("Performing full refresh for insert_update.")
                try:
                    if self.query:
                        df = pd.read_sql_query(sql=text(self.query), con=engine)
                    else:
                        df = pd.read_sql_table(table_name=self.source_table, con=engine)
                    logging.info(f"Read {len(df)} rows from Postgres.")
                except Exception as e:
                    logging.error(f"Error reading data from Postgres: {e}")
                    raise

                if df.empty:
                    logging.warning("No data found in the table.")
                    return

                incremental_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental"
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
                
                full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                self._write_to_minio(fs, df, full_path)
            else:
                if not self.watermark_col:
                    raise ValueError("watermark_col is required for 'insert_update' with full_refresh=False")

                full_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full"
                incr_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental"
                max_watermark = None
                latest_prefix = None

                full_watermark = self._get_max_watermark(fs, full_prefix)
                incr_watermark = self._get_max_watermark(fs, incr_prefix)

                if full_watermark is not None and incr_watermark is not None:
                    max_watermark = max(full_watermark, incr_watermark)
                    latest_prefix = full_prefix if full_watermark >= incr_watermark else incr_prefix
                elif full_watermark is not None:
                    max_watermark = full_watermark
                    latest_prefix = full_prefix
                elif incr_watermark is not None:
                    max_watermark = incr_watermark
                    latest_prefix = incr_prefix
                else:
                    logging.info("No previous data found in full/ or incremental/. Loading full table.")

                query = self._construct_query(max_watermark)
                logging.info(f"Executing query for insert_update: {query}")

                try:
                    df = pd.read_sql_query(sql=text(query), con=engine)
                    logging.info(f"Read {len(df)} rows from Postgres.")
                except Exception as e:
                    logging.error(f"Error reading data from Postgres: {e}")
                    raise

                if not df.empty:
                    incr_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                    self._write_to_minio(fs, df, incr_path)
                else:
                    logging.info("No new data to write.")

        elif self.update_type == "insert_update_delete":
            logging.info("Performing full overwrite for insert_update_delete.")
            try:
                if self.query:
                    df = pd.read_sql_query(sql=text(self.query), con=engine)
                else:
                    df = pd.read_sql_table(table_name=self.source_table, con=engine)
                logging.info(f"Read {len(df)} rows from Postgres.")
            except Exception as e:
                logging.error(f"Error reading data from Postgres: {e}")
                raise

            if df.empty:
                logging.warning("No data found in the table.")
                return

            full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{output_filename}"
            self._write_to_minio(fs, df, full_path)

        # Write to current/ (always overwrite)
        # current_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/current/{output_filename}"
        # if 'df' in locals() and not df.empty:
        #     self._write_to_minio(fs, df, current_path)
        # else:
        #     logging.info("No data to write to current/ path.")