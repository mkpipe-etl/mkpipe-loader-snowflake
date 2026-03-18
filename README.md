# mkpipe-loader-snowflake

Snowflake loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into Snowflake tables using the native Snowflake Spark connector (`spark-snowflake`), which stages data via internal cloud storage (S3/Azure/GCS) — significantly faster than JDBC for large datasets.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  snowflake_target:
    variant: snowflake
    host: myaccount.snowflakecomputing.com
    port: 443
    database: MY_DATABASE
    schema: MY_SCHEMA
    user: myuser
    password: mypassword
    warehouse: MY_WAREHOUSE
```

With RSA key pair authentication:

```yaml
connections:
  snowflake_target:
    variant: snowflake
    host: myaccount.snowflakecomputing.com
    port: 443
    database: MY_DATABASE
    schema: MY_SCHEMA
    user: myuser
    warehouse: MY_WAREHOUSE
    private_key_file: /path/to/rsa_key.p8
    private_key_file_pwd: mypassphrase
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_snowflake
    source: pg_source
    destination: snowflake_target
    tables:
      - name: public.events
        target_name: STG_EVENTS
        replication_method: full
        batchsize: 50000
```

---

## Write Parallelism & Throughput

Snowflake loader uses the native Spark connector. Two parameters control write performance:

```yaml
      - name: public.events
        target_name: STG_EVENTS
        replication_method: full
        batchsize: 50000        # rows per batch insert (default: 10000)
        write_partitions: 4     # coalesce DataFrame to N partitions before writing
```

### How they work

- **`batchsize`**: number of rows buffered before sending to Snowflake. Larger batches reduce round-trips and staging overhead.
- **`write_partitions`**: calls `coalesce(N)` on the DataFrame before writing, controlling the number of concurrent write operations to Snowflake.

### Performance Notes

- **Snowflake Warehouse size** is the primary write performance lever. A larger warehouse processes inserts faster regardless of partition count.
- The Spark connector stages data internally before committing. Large `batchsize` (50,000+) reduces staging overhead.
- For very large loads, consider using Snowflake's native `COPY INTO` via an external stage (S3/GCS) instead — that is significantly faster but requires additional infrastructure.
- `write_partitions: 4–8` is a good default to balance throughput and connection count.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | Snowflake destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `batchsize` | int | `10000` | Rows per batch insert |
| `write_partitions` | int | — | Coalesce DataFrame to N partitions before writing |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |