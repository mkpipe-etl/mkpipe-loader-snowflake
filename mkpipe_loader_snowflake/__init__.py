import gc
from datetime import datetime

from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
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

    def _base_options(self) -> dict:
        opts = {
            'sfURL': f'{self.host}:{self.port}',
            'sfUser': self.username,
            'sfDatabase': self.database,
            'sfSchema': self.schema,
            'sfWarehouse': self.warehouse,
        }
        if self.private_key_file:
            opts['pem_private_key'] = self.private_key_file
            if self.private_key_file_pwd:
                opts['sfPrivateKeyPassphrase'] = self.private_key_file_pwd
        else:
            opts['sfPassword'] = self.password
        return opts

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        logger.info({'table': target_name, 'status': 'loading', 'write_mode': write_mode})

        opts = {**self._base_options(), 'dbtable': target_name}
        writer = df.write.format('net.snowflake.spark.snowflake').mode(write_mode)
        for k, v in opts.items():
            writer = writer.option(k, v)
        writer.save()

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
