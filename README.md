# POSTGRES-KAFKA-ICEBERG-PIPELINE

Example pipeline to stream the data changes from RDBMS to Apache Iceberg tables stored on an S3 compatible object store (MinIO) and tracked in a Hive metastore under two approaches.

1. Manually processing the Kafka messages via Spark
2. Using the Apache Iceberg Kafka Connect sink connector

https://github.com/waiyan1612/postgres-kafka-iceberg-pipeline/assets/8967715/91d59de8-60e2-4e4c-b54a-47c1d36039a3

## Contents
```sh
├── docker-compose.yaml                        -> Compose file to launch postgres, kafka, minio, hive metastore and spark containers
├── pom.xml                                    -> Maven descriptor listing the required libraries
├── kafka
│   ├── config
│   │   ├── connect-file-sink.properties       -> File sink connector
│   │   ├── connect-iceberg-sink.properties    -> Iceberg Sink connector
│   │   ├── connect-postgres-source.json       -> Postgres source connector
│   │   └── connect-standalone.properties      -> Standalone kafka conenct config
│   └── plugins
│       └── debezium-connector-postgres        -> Populated by Maven with Debezium connector jars
│       └── iceberg-kafka-connect              -> Populated by Maven with Apache Iceberg Kafka Connect jars
├── postgres
│   ├── postgresql.conf                        -> Config with logical replication enabled
│   └── scripts
│       ├── manual
│       └── seed                               -> SQL scripts that will be run the first time the db is created i.e. when the data directory is empty
└── spark
    ├── .ivy                                   -> Kafka and iceberg dependency jars will be downloaded to this folder
    └── scripts
        ├── consumer.py                        -> Pyspark script to consume kafka messages and stream to console and iceberg sinks
        └── print_iceberg_tables.py            -> Pyspark script to query the tables created by Spark/Iceberg Sink Kafka connector

```
The Kafka file sink writes to the host path below while all Iceberg table data is stored in MinIO.
```sh
├── data
│   └── kafka
│       └── out
│          └── cdc.commerce.sink.txt           -> Output of Kafka File sink connector
```

## Setup Guide

1. Run the setup script. It uses Maven (via `pom.xml`) to download the Kafka Connect plugins, starts all services, creates the MinIO warehouse bucket, and launches the Kafka connectors.

    ```sh
    ./setup.sh
    ```

2. If we are running for the first time, the spark container will need to download the required dependencies. We need to wait for the downloads to complete before we can proceed. We can monitor the stdout to see if the downloads are completed. Alternatively, we can also query to see the spark streaming app is already running.
    ```sh
    docker container exec kafka-spark curl -s http://localhost:4040/api/v1/applications | jq
    ```
    **Reponse**
    ```json
    [
      {
        "id": "local-1713191253855",
        "name": "cdc-consumer",
        "attempts": [
          {
            "startTime": "2024-04-15T14:27:32.908GMT",
            "endTime": "1969-12-31T23:59:59.999GMT",
            "lastUpdated": "2024-04-15T14:27:32.908GMT",
            "duration": 455827,
            "sparkUser": "root",
            "completed": false,
            "appSparkVersion": "3.5.1",
            "startTimeEpoch": 1713191252908,
            "endTimeEpoch": -1,
            "lastUpdatedEpoch": 1713191252908
          }
        ]
      }
    ]
    ```

3. Check the Iceberg tables from the sinks.
    1. Check the iceberg tables created by Spark.
        ```sh
        docker container exec kafka-spark \
          /opt/spark/bin/spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws:1.6.1,org.apache.iceberg:iceberg-hive-metastore:1.6.1 \
          --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/.ivy -Divy.home=/.ivy" \
          /scripts/print_iceberg_tables.py spark
        ```
        **Output**
        ```
        +------+-----------------------------------------------------------------------+---+
        |before|after                                                                  |op |
        +------+-----------------------------------------------------------------------+---+
        |NULL  |{"user_id":1,"email":"alice@example.com","created_at":1713192083639740}|r  |
        |NULL  |{"user_id":2,"email":"bob@example.com","created_at":1713192083639740}  |r  |
        |NULL  |{"user_id":3,"email":"carol@example.com","created_at":1713192083639740}|r  |
        +------+-----------------------------------------------------------------------+---+

        +------+----------------------------------------------------------------------------------------+---+
        |before|after                                                                                   |op |
        +------+----------------------------------------------------------------------------------------+---+
        |NULL  |{"product_id":1,"product_name":"Live Edge Dining Table","created_at":1713192083641523}  |r  |
        |NULL  |{"product_id":2,"product_name":"Simple Teak Dining Chair","created_at":1713192083641523}|r  |
        +------+----------------------------------------------------------------------------------------+---+
        ```
    2. Check the iceberg tables created by Iceberg sink kafka connector.
        ```sh
        docker container exec kafka-spark \
        /opt/spark/bin/spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws:1.6.1,org.apache.iceberg:iceberg-hive-metastore:1.6.1 \
          --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/.ivy -Divy.home=/.ivy" \
          /scripts/print_iceberg_tables.py kafka
        ```
        **Output**
        ```
        +-------+-----------------+----------------+------------------------------------------------------------------------+
        |user_id|email            |created_at      |_cdc                                                                    |
        +-------+-----------------+----------------+------------------------------------------------------------------------+
        |1      |alice@example.com|1713192083639740|{I, 2024-04-15 14:43:04.407, 0, commerce.account, commerce.account, {1}}|
        |2      |bob@example.com  |1713192083639740|{I, 2024-04-15 14:43:04.411, 1, commerce.account, commerce.account, {2}}|
        |3      |carol@example.com|1713192083639740|{I, 2024-04-15 14:43:04.411, 2, commerce.account, commerce.account, {3}}|
        +-------+-----------------+----------------+------------------------------------------------------------------------+

        +----------+------------------------+----------------+------------------------------------------------------------------------+
        |product_id|product_name            |created_at      |_cdc                                                                    |
        +----------+------------------------+----------------+------------------------------------------------------------------------+
        |1         |Live Edge Dining Table  |1713192083641523|{I, 2024-04-15 14:43:04.417, 0, commerce.product, commerce.product, {1}}|
        |2         |Simple Teak Dining Chair|1713192083641523|{I, 2024-04-15 14:43:04.417, 1, commerce.product, commerce.product, {2}}|
        +----------+------------------------+----------------+------------------------------------------------------------------------+
        ```
    3. We can also run the pyspark shell to debug or run further analysis.
        ```sh
        docker container exec -it kafka-spark /opt/spark/bin/pyspark \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws:1.6.1,org.apache.iceberg:iceberg-hive-metastore:1.6.1 \
        --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/.ivy -Divy.home=/.ivy"
        ```
4. Run the CUD operations on postgres. Repeat step 3-4.

## Helpful Commands for Debugging

### One-liner to wipe data directory while keeping .gitkeep files
```sh
find ./data ! -name .gitkeep -delete
```

### List kafka topics
```sh
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Kafka Connect Endpoints 
```sh
# Health check
curl localhost:8083

# Check plugins
curl localhost:8083/connector-plugins

# Check connectors - configs, status, topics 
curl localhost:8083/connectors
curl localhost:8083/connectors/dbz-pg-source | jq
curl localhost:8083/connectors/dbz-pg-source/status | jq
curl localhost:8083/connectors/dbz-pg-source/topics | jq

# Pause, resume, restart, stop or delete connectors
curl -X PUT localhost:8083/connectors/dbz-pg-source/pause
curl -X PUT localhost:8083/connectors/dbz-pg-source/resume
curl -X PUT localhost:8083/connectors/dbz-pg-source/restart
curl -X PUT localhost:8083/connectors/dbz-pg-source/stop
curl -X DELETE localhost:8083/connectors/dbz-pg-source
```
