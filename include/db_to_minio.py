from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.oracle.hooks.oracle import OracleHook
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

class DatabaseToMinioParquetOperator(BaseOperator):
    """
    Loads data from a database table (Postgres, MSSQL, Oracle, MySQL) to MinIO as Parquet file(s)
    with support for different update types and batch processing.

    Args:
        source_db_type (str): Type of source database ('postgres', 'mssql', 'oracle', 'mysql').
        conn_id (str): Airflow connection ID for the database.
        source_table (str): Name of the source table.
        minio_endpoint_url (str): MinIO endpoint URL.
        minio_access_key (str): MinIO access key.
        minio_secret_key (str): MinIO secret key.
        minio_bucket (str): MinIO bucket name.
        goo (str): Goo identifier for path.
        iss (str): ISS identifier for path.
        update_type (str): Type of update ('insert', 'insert_update', 'insert_update_delete').
        full_refresh (bool): Whether to perform a full refresh for 'insert_update' case.
        watermark_col (Optional[str]): Column used for incremental updates.
        custom_sql (Optional[str]): Custom SQL query to extract data.
        query (Optional[str]): Deprecated custom SQL query (use custom_sql instead).
        table_name (Optional[str]): Custom table name for output file.
        partition_cols (Optional[list]): Columns to partition data by in MinIO.
        use_threads (bool): Whether to use threads for S3 transfer.
        batch_size (Optional[int]): Number of rows per batch for processing.
    """

    @apply_defaults
    def __init__(
        self,
        source_db_type: str,
        conn_id: str,
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
        custom_sql: Optional[str] = None,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
        partition_cols: Optional[list] = None,
        use_threads: bool = True,
        batch_size: Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_db_type = source_db_type.lower()
        self.conn_id = conn_id
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
        self.custom_sql = custom_sql
        self.query = query
        self.table_name = table_name
        self.partition_cols = partition_cols
        self.use_threads = use_threads
        self.batch_size = batch_size

        # Validate source_db_type
        valid_db_types = ["postgres", "mssql", "oracle", "mysql"]
        if self.source_db_type not in valid_db_types:
            raise ValueError(f"Invalid source_db_type: {self.source_db_type}. Must be one of {valid_db_types}")

        # Validate update_type
        valid_update_types = ["insert", "insert_update", "insert_update_delete"]
        if self.update_type not in valid_update_types:
            raise ValueError(f"Invalid update_type: {self.update_type}. Must be one of {valid_update_types}")

        # Validate source_table
        if not self.source_table:
            raise ValueError("source_table is required for default query and path construction")

    def _get_s3fs(self):
        """Initialize and return s3fs filesystem."""
        return s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": self.minio_endpoint_url},
            key=self.minio_access_key,
            secret=self.minio_secret_key,
        )

    def _get_db_hook(self):
        """Return the appropriate Airflow hook based on source_db_type."""
        if self.source_db_type == "postgres":
            return PostgresHook(postgres_conn_id=self.conn_id)
        # elif self.source_db_type == "mssql":
        #     return MsSqlHook(mssql_conn_id=self.conn_id)
        # elif self.source_db_type == "mysql":
        #     return MySqlHook(mysql_conn_id=self.conn_id)
        # elif self.source_db_type == "oracle":
        #     return OracleHook(oracle_conn_id=self.conn_id)
        else:
            raise ValueError(f"Unsupported source_db_type: {self.source_db_type}")

    def _get_sqlalchemy_engine(self):
        """Create and return SQLAlchemy engine for the database."""
        hook = self._get_db_hook()
        conn = hook.get_conn()
        if self.source_db_type == "postgres":
            conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        elif self.source_db_type == "mssql":
            conn_str = f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
        elif self.source_db_type == "mysql":
            conn_str = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        elif self.source_db_type == "oracle":
            conn_str = f"oracle+cx_oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        return create_engine(conn_str)

    def _get_max_watermark(self, fs, path_prefix: str, find_latest_day: bool = False) -> Optional[any]:
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

    def _construct_query(self, max_watermark: Optional[any] = None) -> str:
        """
        Construct the SQL query based on the update type and watermark.
        """
        if self.custom_sql:
            base_query = self.custom_sql
        elif self.query:
            base_query = self.query
        else:
            base_query = f"SELECT * FROM {self.source_table}"

        if max_watermark is not None and self.watermark_col:
            if isinstance(max_watermark, (int, float)):
                condition = f"WHERE {self.watermark_col} > {max_watermark}"
            else:
                condition = f"WHERE {self.watermark_col} > '{max_watermark}'"
            return f"{base_query} {condition}"
        return base_query

    def _write_to_minio(self, fs, df: pd.DataFrame, output_path: str, batch_index: Optional[int] = None):
        """
        Write DataFrame to MinIO as Parquet, optionally appending batch index to filename.
        """
        try:
            df = df.copy()
            if self.partition_cols:
                for col in self.partition_cols:
                    df[col] = df[col].apply(sanitize_minio_path)

                # Adjust output path for batch processing
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
        Execute the operator with batch processing support.
        """
        fs = self._get_s3fs()
        engine = self._get_sqlalchemy_engine()

        # Determine output filename
        output_filename = self.table_name or self.source_table
        if self.custom_sql or self.query:
            import hashlib
            sql = self.custom_sql or self.query
            query_hash = hashlib.md5(sql.encode()).hexdigest()
            output_filename = f"query_{query_hash}"
        output_filename = f"{output_filename}.parquet"

        dt = datetime(2025, 3, 20)  # For testing purposes
        date_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"

        if self.update_type == "insert":
            if not self.watermark_col:
                raise ValueError("watermark_col is required for 'insert' update_type")

            path_prefix = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}"
            max_watermark = self._get_max_watermark(fs, path_prefix, find_latest_day=True)

            query = self._construct_query(max_watermark)
            logging.info(f"Executing query for insert: {query}")

            try:
                if self.batch_size:
                    batch_index = 0
                    for chunk in pd.read_sql_query(sql=text(query), con=engine, chunksize=self.batch_size):
                        logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                        if not chunk.empty:
                            self._write_to_minio(fs, chunk, date_path, batch_index)
                            batch_index += 1
                        else:
                            logging.info("Empty batch encountered.")
                    if batch_index == 0:
                        logging.info("No data to write to MinIO.")
                else:
                    df = pd.read_sql_query(sql=text(query), con=engine)
                    logging.info(f"Read {len(df)} rows from {self.source_db_type}.")
                    if not df.empty:
                        self._write_to_minio(fs, df, date_path)
                    else:
                        logging.info("No data to write to MinIO.")
            except Exception as e:
                logging.error(f"Error reading data from {self.source_db_type}: {e}")
                raise

        elif self.update_type == "insert_update":
            if self.full_refresh:
                logging.info("Performing full refresh for insert_update.")
                try:
                    if self.batch_size:
                        batch_index = 0
                        if self.custom_sql or self.query:
                            sql = self.custom_sql or self.query
                            for chunk in pd.read_sql_query(sql=text(sql), con=engine, chunksize=self.batch_size):
                                logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                                if not chunk.empty:
                                    full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                                    self._write_to_minio(fs, chunk, full_path, batch_index)
                                    batch_index += 1
                                else:
                                    logging.info("Empty batch encountered.")
                            if batch_index == 0:
                                logging.warning("No data found in the table.")
                                return
                        else:
                            for chunk in pd.read_sql_table(table_name=self.source_table, con=engine, chunksize=self.batch_size):
                                logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                                if not chunk.empty:
                                    full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                                    self._write_to_minio(fs, chunk, full_path, batch_index)
                                    batch_index += 1
                                else:
                                    logging.info("Empty batch encountered.")
                            if batch_index == 0:
                                logging.warning("No data found in the table.")
                                return
                    else:
                        if self.custom_sql or self.query:
                            sql = self.custom_sql or self.query
                            df = pd.read_sql_query(sql=text(sql), con=engine)
                        else:
                            df = pd.read_sql_table(table_name=self.source_table, con=engine)
                        logging.info(f"Read {len(df)} rows from {self.source_db_type}.")
                        if df.empty:
                            logging.warning("No data found in the table.")
                            return
                        full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                        self._write_to_minio(fs, df, full_path)
                except Exception as e:
                    logging.error(f"Error reading data from {self.source_db_type}: {e}")
                    raise

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
                    if self.batch_size:
                        batch_index = 0
                        for chunk in pd.read_sql_query(sql=text(query), con=engine, chunksize=self.batch_size):
                            logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                            if not chunk.empty:
                                incr_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                                self._write_to_minio(fs, chunk, incr_path, batch_index)
                                batch_index += 1
                            else:
                                logging.info("Empty batch encountered.")
                        if batch_index == 0:
                            logging.info("No new data to write.")
                    else:
                        df = pd.read_sql_query(sql=text(query), con=engine)
                        logging.info(f"Read {len(df)} rows from {self.source_db_type}.")
                        if not df.empty:
                            incr_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/incremental/{dt.strftime('%Y')}/{dt.strftime('%m')}/{dt.strftime('%d')}/{output_filename}"
                            self._write_to_minio(fs, df, incr_path)
                        else:
                            logging.info("No new data to write.")
                except Exception as e:
                    logging.error(f"Error reading data from {self.source_db_type}: {e}")
                    raise

        elif self.update_type == "insert_update_delete":
            logging.info("Performing full overwrite for insert_update_delete.")
            try:
                if self.batch_size:
                    batch_index = 0
                    if self.custom_sql or self.query:
                        sql = self.custom_sql or self.query
                        for chunk in pd.read_sql_query(sql=text(sql), con=engine, chunksize=self.batch_size):
                            logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                            if not chunk.empty:
                                full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{output_filename}"
                                self._write_to_minio(fs, chunk, full_path, batch_index)
                                batch_index += 1
                            else:
                                logging.info("Empty batch encountered.")
                        if batch_index == 0:
                            logging.warning("No data found in the table.")
                            return
                    else:
                        for chunk in pd.read_sql_table(table_name=self.source_table, con=engine, chunksize=self.batch_size):
                            logging.info(f"Processing batch {batch_index} with {len(chunk)} rows.")
                            if not chunk.empty:
                                full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{output_filename}"
                                self._write_to_minio(fs, chunk, full_path, batch_index)
                                batch_index += 1
                            else:
                                logging.info("Empty batch encountered.")
                        if batch_index == 0:
                            logging.warning("No data found in the table.")
                            return
                else:
                    if self.custom_sql or self.query:
                        sql = self.custom_sql or self.query
                        df = pd.read_sql_query(sql=text(sql), con=engine)
                    else:
                        df = pd.read_sql_table(table_name=self.source_table, con=engine)
                    logging.info(f"Read {len(df)} rows from {self.source_db_type}.")
                    if df.empty:
                        logging.warning("No data found in the table.")
                        return
                    full_path = f"{self.minio_bucket}/{self.goo}/{self.iss}/{self.source_table}/full/{output_filename}"
                    self._write_to_minio(fs, df, full_path)
            except Exception as e:
                logging.error(f"Error reading data from {self.source_db_type}: {e}")
                raise