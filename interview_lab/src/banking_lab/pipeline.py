"""Pipeline batch de treinamento: schema, qualidade, deduplicacao e reconciliacao."""
from pyspark.sql import DataFrame, Window, functions as F, types as T

RAW_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("amount_str", T.StringType()),
    T.StructField("event_ts_str", T.StringType()),
    T.StructField("ingested_at_str", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("_corrupt_record", T.StringType()),
])
ALLOWED_STATUSES = ("APPROVED", "PENDING", "CANCELLED")
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def read_raw_csv(spark, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_SCHEMA).csv(path)
    )


def normalize_and_validate(raw: DataFrame) -> DataFrame:
    prepared = (
        raw.withColumn("event_id", F.trim("event_id"))
        .withColumn("transaction_id", F.trim("transaction_id"))
        .withColumn("account_id", F.trim("account_id"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("amount_raw", F.trim("amount_str"))
        .withColumn("amount", F.expr("try_cast(amount_str as decimal(18,2))"))
        .withColumn("event_ts", F.try_to_timestamp("event_ts_str", F.lit(TIMESTAMP_FORMAT)))
        .withColumn("ingested_at", F.try_to_timestamp("ingested_at_str", F.lit(TIMESTAMP_FORMAT)))
    )
    amount_ok = F.col("amount_raw").rlike(r"^[0-9]+(\.[0-9]{1,2})?$")
    errors = [
        F.when(F.col("_corrupt_record").isNotNull(), "CSV_CORRUPT"),
        F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), "EVENT_ID_MISSING"),
        F.when(F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), "TRANSACTION_ID_MISSING"),
        F.when(F.col("account_id").isNull() | (F.col("account_id") == ""), "ACCOUNT_ID_MISSING"),
        F.when(~amount_ok | F.col("amount").isNull() | (F.col("amount") <= 0), "INVALID_AMOUNT"),
        F.when(F.col("event_ts").isNull(), "INVALID_EVENT_TIME"),
        F.when(F.col("ingested_at").isNull(), "INVALID_INGEST_TIME"),
        F.when(F.col("status").isNull() | ~F.col("status").isin(*ALLOWED_STATUSES), "INVALID_STATUS"),
    ]
    return (
        prepared.withColumn("error_reasons", F.concat_ws("|", *errors))
        .withColumn("is_valid", F.col("error_reasons") == "")
    )


def split_valid_invalid(df: DataFrame):
    return df.filter(F.col("is_valid")), df.filter(~F.col("is_valid"))


def latest_by_transaction(valid: DataFrame) -> DataFrame:
    """Uma linha por transaction_id; empate resolvido por ordem total documentada."""
    hashed = valid.withColumn(
        "_payload_hash",
        F.sha2(F.to_json(F.struct(
            "event_id", "transaction_id", "account_id",
            "amount", "event_ts", "ingested_at", "status"
        )), 256),
    )
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("event_ts").desc(),
        F.col("ingested_at").desc(),
        F.col("event_id").desc(),
        F.col("_payload_hash").desc(),
    )
    return (
        hashed.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_payload_hash")
    )


def account_daily_totals(latest: DataFrame) -> DataFrame:
    """Regra ficticia: somente o estado final APPROVED compoe o total."""
    return (
        latest.filter(F.col("status") == "APPROVED")
        .withColumn("business_date", F.to_date("event_ts"))
        .groupBy("account_id", "business_date")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount").cast("decimal(20,2)").alias("total_amount"),
        )
    )


def join_account_dimension(latest: DataFrame, accounts: DataFrame) -> DataFrame:
    """Nao permite multiplicacao de fatos por chaves duplicadas da dimensao."""
    if accounts.groupBy("account_id").count().filter("count > 1").limit(1).count():
        raise ValueError("DUPLICATE_ACCOUNT_ID_IN_DIMENSION")
    joined = latest.join(accounts, "account_id", "left")
    if joined.count() != latest.count():
        raise RuntimeError("JOIN_CARDINALITY_VIOLATION")
    return joined


def validate_reconciliation(valid: DataFrame, silver: DataFrame) -> dict:
    events = valid.count()
    unique_ids = valid.select("transaction_id").distinct().count()
    silver_count = silver.count()
    if silver_count != unique_ids or silver_count > events:
        raise RuntimeError("RECONCILIATION_FAILED")
    return {
        "valid_event_count": events,
        "unique_transaction_count": unique_ids,
        "silver_count": silver_count,
        "deduplicated_events": events - silver_count,
    }
