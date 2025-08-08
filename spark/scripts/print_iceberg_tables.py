import sys
import pyspark
from pyspark.sql import SparkSession

MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
HIVE_METASTORE_URI = 'thrift://hive-metastore:9083'

catalog_type = sys.argv[1]
if catalog_type == 'kafka':
    warehouse_path = 's3://warehouse/kafka/iceberg/warehouse'
elif catalog_type == 'spark':
    warehouse_path = 's3://warehouse/spark/iceberg/warehouse'
else:
    raise ValueError('Invalid catalog type. Use either kafka or spark')

conf = (pyspark.SparkConf()
    .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .set('spark.sql.catalog.iceberg', 'org.apache.iceberg.spark.SparkCatalog')
    .set('spark.sql.catalog.iceberg.type', 'hive')
    .set('spark.sql.catalog.iceberg.uri', HIVE_METASTORE_URI)
    .set('spark.sql.catalog.iceberg.warehouse', warehouse_path)
    .set('spark.sql.catalog.iceberg.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .set('spark.sql.catalog.iceberg.s3.endpoint', MINIO_ENDPOINT)
    .set('spark.sql.catalog.iceberg.s3.access-key-id', MINIO_ACCESS_KEY)
    .set('spark.sql.catalog.iceberg.s3.secret-access-key', MINIO_SECRET_KEY)
    .set('spark.sql.catalog.iceberg.s3.path-style-access', 'true')
)

spark = (SparkSession.builder
    .master('local')
    .appName('cdc-consumer')
    .config(conf=conf)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

spark.read.table('iceberg.cdc.commerce_account').show(truncate=False)
spark.read.table('iceberg.cdc.commerce_product').show(truncate=False)
