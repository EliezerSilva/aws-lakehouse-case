"""Glue Spark job - valida Silver e grava rejeitados (laboratorio)."""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, ["TARGET_BUCKET"])
spark = SparkSession.builder.getOrCreate()
bucket = args['TARGET_BUCKET']
df = spark.read.parquet(f"s3://{bucket}/bronze/locacoes/")
df = (df.withColumn("valor_total_num", F.col("valor_total").cast("decimal(18,2)"))
        .withColumn("data_inicio_date", F.to_date("data_inicio")))
valid = (F.col("id_locacao").isNotNull() & F.col("valor_total_num").isNotNull()
         & (F.col("valor_total_num") >= 0) & F.col("data_inicio_date").isNotNull())
clean = df.filter(valid).dropDuplicates(["id_locacao"])
bad = df.filter(~F.coalesce(valid, F.lit(False)))
clean.write.mode("overwrite").parquet(f"s3://{bucket}/silver/locacoes/")
bad.write.mode("overwrite").parquet(f"s3://{bucket}/quarantine/locacoes/")
print(f"Silver: {clean.count()} / quarentena: {bad.count()}")
# A checagem de chaves orfas e deduplicacao por evento sao passos futuros.
