"""Tratamento de locacoes Silver executavel no AWS Glue."""

import sys

from pyspark.sql import functions as F


BUSINESS_COLUMNS = [
    "id_locacao",
    "id_reserva",
    "id_cliente",
    "id_veiculo",
    "id_loja",
    "data_inicio",
    "valor_total",
    "status",
]


def transform_locacoes(df):
    """Classifica registros e separa Silver e quarentena."""

    prepared = (
        df.withColumn(
            "valor_total_num",
            F.col("valor_total").cast("decimal(18,2)")
        )
        .withColumn(
            "data_inicio_date",
            F.to_date("data_inicio")
        )
    )

    reason = (
        F.when(
            F.col("id_locacao").isNull()
            | (F.trim(F.col("id_locacao")) == ""),
            F.lit("ID_INVALIDO")
        )
        .when(
            F.col("valor_total_num") < 0,
            F.lit("VALOR_NEGATIVO")
        )
        .when(
            F.col("valor_total_num").isNull(),
            F.lit("VALOR_INVALIDO")
        )
        .when(
            F.col("data_inicio_date").isNull(),
            F.lit("DATA_INVALIDA")
        )
    )

    classified = prepared.withColumn("rejection_reason", reason)

    invalid = classified.filter(
        F.col("rejection_reason").isNotNull()
    )

    valid = classified.filter(
        F.col("rejection_reason").isNull()
    ).drop("rejection_reason")

    distinct_versions = valid.select(
        *BUSINESS_COLUMNS
    ).distinct()

    conflicting_ids = (
        distinct_versions
        .groupBy("id_locacao")
        .count()
        .filter(F.col("count") > 1)
        .select("id_locacao")
    )

    conflicts = (
        valid.join(conflicting_ids, "id_locacao", "inner")
        .withColumn(
            "rejection_reason",
            F.lit("DUPLICIDADE_CONFLITANTE")
        )
    )

    clean = (
        valid.join(conflicting_ids, "id_locacao", "left_anti")
        .dropDuplicates(BUSINESS_COLUMNS)
    )

    quarantine = invalid.unionByName(conflicts)

    return clean, quarantine


def main():
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(sys.argv, ["TARGET_BUCKET"])
    spark = SparkSession.builder.getOrCreate()
    bucket = args["TARGET_BUCKET"]

    df = spark.read.parquet(
        f"s3://{bucket}/bronze/locacoes/"
    )

    clean, quarantine = transform_locacoes(df)

    bronze_count = df.count()
    silver_count = clean.count()
    quarantine_count = quarantine.count()
    duplicates_removed = (
        bronze_count - silver_count - quarantine_count
    )

    if duplicates_removed < 0:
        raise ValueError("Falha na reconciliacao de registros")

    if bronze_count != silver_count + quarantine_count + duplicates_removed:
        raise ValueError("Contagens inconsistentes")

    print(
        f"Bronze={bronze_count}; "
        f"Silver={silver_count}; "
        f"Quarantine={quarantine_count}; "
        f"ExactDuplicates={duplicates_removed}"
    )

    clean.write.mode("overwrite").parquet(
        f"s3://{bucket}/silver/locacoes/"
    )

    quarantine.write.mode("overwrite").parquet(
        f"s3://{bucket}/quarantine/locacoes/"
    )


if __name__ == "__main__":
    main()
