"""Glue Spark job - agregacao Gold de receita por loja e data."""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F
args = getResolvedOptions(sys.argv, ["TARGET_BUCKET"])
spark = SparkSession.builder.getOrCreate()
b = args['TARGET_BUCKET']
df = spark.read.parquet(f"s3://{b}/silver/locacoes/")
result = (df.groupBy("id_loja", "data_inicio_date")
            .agg(F.count("id_locacao").alias("total_locacoes"),
                 F.sum("valor_total_num").alias("receita_total")))
result.write.mode("overwrite").parquet(f"s3://{b}/gold/receita_diaria_loja/")
print(f"Gold criada: {result.count()} linhas")
