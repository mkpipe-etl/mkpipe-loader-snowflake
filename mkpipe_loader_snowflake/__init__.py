import gc
from datetime import datetime
from typing import List, Optional

from mkpipe.exceptions import ConfigError, LoadError
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig, WriteStrategy
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.strategy import resolve_write_strategy
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'net.snowflake:spark-snowflake_2.13:3.1.0',
    'net.snowflake:snowflake-jdbc:3.24.0',
]

logger = get_logger(__name__)


class SnowflakeLoader(BaseLoader, variant='snowflake'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host
        self.port = connection.port or 443
        self.username = connection.user
        self.password = str(connection.password or '')
        self.database = connection.database
        self.schema = connection.schema or 'PUBLIC'
        self.warehouse = connection.warehouse
        self.private_key_file = connection.private_key_file
        self.private_key_file_pwd = connection.private_key_file_pwd

    @staticmethod
    def _read_pem_key(key_path: str, passphrase: Optional[str] = None) -> str:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        with open(key_path, 'rb') as f:
            key_data = f.read()

        pwd_bytes = passphrase.encode() if passphrase else None
        private_key = serialization.load_pem_private_key(
            key_data, password=pwd_bytes, backend=default_backend()
        )
        key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        # Strip PEM header/footer and newlines to get raw base64 content
        key_str = (
            key_bytes.decode('utf-8')
            .replace('-----BEGIN PRIVATE KEY-----', '')
            .replace('-----END PRIVATE KEY-----', '')
            .strip()
        )
        return key_str

    def _base_options(self) -> dict:
        import os

        opts = {
            'sfURL': f'{self.host}:{self.port}',
            'sfUser': self.username,
            'sfDatabase': self.database,
            'sfSchema': self.schema,
            'sfWarehouse': self.warehouse,
        }
        if self.private_key_file:
            key_path = os.path.expanduser(self.private_key_file)
            opts['pem_private_key'] = self._read_pem_key(
                key_path, self.private_key_file_pwd
            )
        else:
            opts['sfPassword'] = self.password
        return opts

    def _write_df(self, df, write_mode: str, table_name: str) -> None:
        opts = {**self._base_options(), 'dbtable': table_name}
        writer = df.write.format('net.snowflake.spark.snowflake').mode(write_mode)
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()

    def _execute_sql(self, sql: str, spark) -> None:
        opts = self._base_options()
        spark.read.format('net.snowflake.spark.snowflake') \
            .options(**opts) \
            .option('query', sql) \
            .load()

    def _build_merge_sql(
        self,
        temp_table: str,
        target_table: str,
        write_key: List[str],
        columns: List[str],
        update_columns: List[str],
    ) -> str:
        join_cond = ' AND '.join(f't."{k}" = s."{k}"' for k in write_key)
        insert_cols = ', '.join(f'"{c}"' for c in columns)
        insert_vals = ', '.join(f's."{c}"' for c in columns)
        update_set = ', '.join(f'"{c}" = s."{c}"' for c in update_columns)
        return (
            f'MERGE INTO {target_table} AS t '
            f'USING {temp_table} AS s ON {join_cond} '
            f'WHEN MATCHED THEN UPDATE SET {update_set} '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )

    def _upsert(self, df, target_name: str, write_key: List[str], spark) -> None:
        temp_table = f'_mkpipe_tmp_{target_name}'
        try:
            self._write_df(df, 'overwrite', temp_table)
            non_key_cols = [c for c in df.columns if c not in write_key]
            sql = self._build_merge_sql(
                temp_table, target_name, write_key, df.columns, non_key_cols,
            )
            logger.debug({'upsert_sql': sql})
            self._execute_sql(sql, spark)
        finally:
            try:
                self._execute_sql(f'DROP TABLE IF EXISTS {temp_table}', spark)
            except Exception:
                logger.warning("Failed to drop temp table '%s'", temp_table)

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info(
                {'table': target_name, 'status': 'skipped', 'reason': 'no data'}
            )
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        strategy = resolve_write_strategy(table, data)

        logger.info(
            {'table': target_name, 'status': 'loading', 'write_strategy': strategy.value}
        )

        try:
            match strategy:
                case WriteStrategy.APPEND:
                    self._write_df(df, 'append', target_name)
                case WriteStrategy.REPLACE:
                    mode = 'append' if self.if_exists == 'append' else 'overwrite'
                    self._write_df(df, mode, target_name)
                case WriteStrategy.UPSERT | WriteStrategy.MERGE:
                    if not table.write_key:
                        raise ConfigError(
                            f"write_strategy '{strategy.value}' requires write_key "
                            f"for table '{target_name}'"
                        )
                    self._upsert(df, target_name, table.write_key, spark)
                case _:
                    raise ConfigError(
                        f"Snowflake loader does not support write_strategy: {strategy.value}"
                    )
        except (ConfigError, LoadError):
            raise
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
