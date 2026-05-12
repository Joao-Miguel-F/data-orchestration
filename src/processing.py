import os
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def create_spark_session(app_name: str = "PNCP-Pipeline") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.debug.maxToStringFields", "50")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_from_records(spark: SparkSession, registros: list[dict]) -> DataFrame:
    import tempfile, os
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        json.dump(registros, f, ensure_ascii=False)
        tmp = f.name
    try:
        df = spark.read.option("multiline", "true").json(tmp)
        df.cache().count()  # materializa em memoria antes de apagar o arquivo
        return df
    finally:
        os.unlink(tmp)


def flatten_and_select(df: DataFrame) -> DataFrame:
    """Seleciona e renomeia os campos relevantes conforme estrutura real da API."""
    return df.select(
        F.col("numeroControlePNCP"),
        # Orgao
        F.col("orgaoEntidade.cnpj").alias("cnpj_orgao"),
        F.col("orgaoEntidade.razaoSocial").alias("razao_social"),
        # Localizacao
        F.col("unidadeOrgao.municipioNome").alias("municipio_nome"),
        F.col("unidadeOrgao.ufSigla").alias("uf"),
        # Datas
        F.col("dataPublicacaoPncp"),
        F.col("anoCompra").alias("ano_compra"),
        # Descricao — usada para classificar o ramo
        F.col("objetoCompra"),
        # Modalidade (campos planos no JSON)
        F.col("modalidadeId").alias("modalidade_id"),
        F.col("modalidadeNome").alias("modalidade_nome"),
        # Situacao (campos planos no JSON)
        F.col("situacaoCompraId").alias("situacao_id"),
        F.col("situacaoCompraNome").alias("situacao_nome"),
        # Valor
        F.col("valorTotalEstimado").cast(DoubleType()),
    )


def classify_ramo_mei(df: DataFrame) -> DataFrame:
    """
    Classifica cada edital em um ramo do MEI com base em palavras-chave do objetoCompra.
    Sem infraestrutura pesada — apenas correspondencia de texto simples.
    """
    objeto = F.lower(F.col("objetoCompra"))
    return df.withColumn(
        "ramo_mei",
        F.when(objeto.rlike("obra|constru|reforma|paviment|calçament|edificaç"), "Obras")
         .when(objeto.rlike("software|sistema|tecnologia|informátic|ti |suporte técnico|hardware"), "TI")
         .when(objeto.rlike("serviç|manutenç|limpeza|vigilânc|conservaç|lavanderia|portaria|copeira"), "Serviços")
         .when(objeto.rlike("aquisiç|forneciment|material|equipament|produto|compra|item|insumo"), "Compras")
         .otherwise("Outros")
    )


def deduplicate(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["numeroControlePNCP"])


def add_data_coleta(df: DataFrame) -> DataFrame:
    return df.withColumn("data_coleta", F.current_timestamp())


def show_summary(df: DataFrame) -> None:
    total = df.count()
    print(f"\n=== Resumo do DataFrame ===")
    print(f"Total de registros: {total}")

    print("\n-- Distribuicao por ramo do MEI --")
    df.groupBy("ramo_mei").count().orderBy(F.desc("count")).show(truncate=False)

    print("-- Distribuicao por modalidade --")
    df.groupBy("modalidade_nome").count().orderBy(F.desc("count")).show(truncate=False)

    print("-- Top 5 orgaos por numero de contratacoes --")
    df.groupBy("razao_social").count().orderBy(F.desc("count")).limit(5).show(truncate=False)

    print("-- Valor total estimado por ramo --")
    linhas = (
        df.groupBy("ramo_mei")
        .agg(F.sum("valorTotalEstimado").alias("total"))
        .orderBy(F.desc("total"))
        .collect()
    )
    for row in linhas:
        valor = row["total"] or 0
        valor_fmt = f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        print(f"  {row['ramo_mei']:<12} {valor_fmt}")


