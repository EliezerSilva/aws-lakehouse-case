"""Transformacoes Silver para dados publicos NYC TLC."""

from pyspark.sql import functions as F


def transform_trips(df, reference_month="2026-01"):
    """Classifica viagens sem descartar ajustes financeiros."""

    duration = (
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    )

    classified = (
        df.withColumn("duration_seconds", duration)
        .withColumn(
            "quality_rejection_reason",
            F.when(
                F.col("tpep_pickup_datetime").isNull(),
                F.lit("PICKUP_NULO"),
            )
            .when(
                F.col("tpep_dropoff_datetime").isNull(),
                F.lit("DROPOFF_NULO"),
            )
            .when(
                F.col("duration_seconds") < 0,
                F.lit("DURACAO_NEGATIVA"),
            ),
        )
        .withColumn(
            "flag_zero_duration_positive_distance",
            (F.col("duration_seconds") == 0)
            & (F.col("trip_distance") > 0),
        )
        .withColumn(
            "flag_long_duration",
            F.col("duration_seconds") > 86400,
        )
        .withColumn(
            "flag_negative_total",
            F.col("total_amount") < 0,
        )
        .withColumn(
            "flag_outside_reference_month",
            F.date_format(
                "tpep_pickup_datetime", "yyyy-MM"
            ) != F.lit(reference_month),
        )
    )

    silver = classified.filter(
        F.col("quality_rejection_reason").isNull()
    ).drop("quality_rejection_reason")

    quarantine = classified.filter(
        F.col("quality_rejection_reason").isNotNull()
    )

    return silver, quarantine
