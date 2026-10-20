-- Troque SEU_BUCKET_DADOS pelo nome real do bucket.
CREATE DATABASE IF NOT EXISTS mobility_bronze;
CREATE DATABASE IF NOT EXISTS mobility_silver;
CREATE DATABASE IF NOT EXISTS mobility_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS mobility_gold.receita_diaria_loja (
 id_loja string,
 data_inicio_date date,
 total_locacoes bigint,
 receita_total decimal(28,2)
)
STORED AS PARQUET
LOCATION 's3://SEU_BUCKET_DADOS/gold/receita_diaria_loja/'
TBLPROPERTIES ('classification'='parquet');

SELECT id_loja, sum(receita_total) AS receita, sum(total_locacoes) AS locacoes
FROM mobility_gold.receita_diaria_loja
GROUP BY id_loja ORDER BY receita DESC;
