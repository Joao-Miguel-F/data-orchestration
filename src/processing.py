import os
import json
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def _short_path(path: str) -> str:
    """Converte caminho Windows para formato 8.3 (sem acentos/espacos)."""
    import ctypes
    buf_size = ctypes.windll.kernel32.GetShortPathNameW(path, None, 0)
    if buf_size == 0:
        return path
    buf = ctypes.create_unicode_buffer(buf_size)
    ctypes.windll.kernel32.GetShortPathNameW(path, buf, buf_size)
    return buf.value


def create_spark_session(app_name: str = "PNCP-Pipeline") -> SparkSession:
    import sys
    spark_tmp = "C:/tmp/spark"
    os.makedirs(spark_tmp, exist_ok=True)

    # Converte caminhos com acentos para formato curto do Windows (8.3)
    python_short = _short_path(sys.executable)
    os.environ["PYSPARK_PYTHON"] = python_short
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_short

    import pyspark
    spark_home = _short_path(str(Path(pyspark.__file__).parent))
    os.environ["SPARK_HOME"] = spark_home

    # Define JAVA_HOME automaticamente se nao estiver definido
    if not os.environ.get("JAVA_HOME"):
        import subprocess
        result = subprocess.run(["where", "java"], capture_output=True, text=True)
        if result.returncode == 0:
            java_exe = result.stdout.strip().splitlines()[0]
            os.environ["JAVA_HOME"] = _short_path(str(Path(java_exe).parent.parent))

    # winutils.exe e obrigatorio para PySpark no Windows
    if not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = "C:/hadoop"
        os.environ["PATH"] = "C:/hadoop/bin;" + os.environ.get("PATH", "")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.debug.maxToStringFields", "50")
        .config("spark.local.dir", spark_tmp)
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


def save_as_csv(df: DataFrame, path: str) -> None:
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.toPandas().to_csv(path, index=False, encoding="utf-8")
    print(f"CSV salvo em: {path}")



