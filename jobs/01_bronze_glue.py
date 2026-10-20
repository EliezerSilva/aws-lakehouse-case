"""Glue Spark job - carrega CSV de locacoes para Parquet Bronze."""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, ["SOURCE_BUCKET", "TARGET_BUCKET"])
spark = SparkSession.builder.getOrCreate()
source = f"s3://{args['SOURCE_BUCKET']}/sources/erp_locacoes_cabecalho_2026.csv"
target = f"s3://{args['TARGET_BUCKET']}/bronze/locacoes/"
df = (spark.read.option("header", "true").option("sep", ";").csv(source))
(df.withColumn("ingested_at", F.current_timestamp())
   .withColumn("source_file", F.input_file_name())
   .write.mode("overwrite").parquet(target))
print(f"Bronze escrita em {target} (registros={df.count()})")
