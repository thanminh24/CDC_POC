# Configurations for the connectors

## connect-postgres-source.json

Debezium postgres connector will create one Kafka topic per one Postgres table. The topics are prefixed by `topic.prefix`, followed by schema name and table name. Example - `cdc.commerce.account`.

## connect-iceberg-sink.json

Here is a brief description of the configs.
- Subscribe to multiple topics using `topics.regex`.
- Transform the Debezium records using the provided SMT. This is controlled by `transforms` and `transforms.{xxx}.type`.
- Set `transforms.debezium.cdc.target.pattern` to `cdc.{db}_{table}` to overwrite the default `{db}.{table}`.
    - `cdc` is the `topic.prefix` we used in connect-postgres-source.json.
- Use `iceberg.tables.route-field` to identify the field that contains the destination table name.
- Configure the Iceberg catalog using the `iceberg.catalog.*` properties. This example uses a Hive catalog with the S3 `S3FileIO`, pointing the warehouse to MinIO and the catalog URI to the Hive metastore service.
- Since we enable automatic table creation, we must set the `id-columns` for **each** table.
    ```
        "iceberg.table.cdc.commerce_account.id-columns": "user_id",
        "iceberg.table.cdc.commerce_product.id-columns": "product_id",
    ```

## connect-standalone.properties

Changed `plugin.path` to include additional jars from `/opt/kafka/plugins`.
