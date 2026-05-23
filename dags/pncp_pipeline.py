from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def ingest_bronze(data_interval_start, data_interval_end, **kwargs):
    from src.ingestion import iter_pages
    from src.loading import inserir_raw

    di = data_interval_start.strftime("%Y%m%d")
    data_final = data_interval_end.strftime("%Y%m%d")

    for pagina, total_paginas, registros in iter_pages(di, data_final):
        print(f"  [{pagina}/{total_paginas}] {len(registros)} registros")
        if registros:
            inserir_raw(registros)


def transform_silver(data_interval_start, data_interval_end, **kwargs):
    from src.loading import buscar_raw_por_periodo, inserir_processados, contar
    from src.processing import (
        create_spark_session,
        load_from_records,
        flatten_and_select,
        classify_ramo_mei,
        deduplicate,
        add_data_coleta,
    )

    di = data_interval_start.strftime("%Y%m%d")
    data_final = data_interval_end.strftime("%Y%m%d")

    registros = buscar_raw_por_periodo(di, data_final)
    if not registros:
        print("Nenhum registro na Bronze para o período.")
        return

    spark = create_spark_session()
    result = (
        load_from_records(spark, registros)
        .transform(flatten_and_select)
        .transform(classify_ramo_mei)
        .transform(deduplicate)
        .transform(add_data_coleta)
    )
    inserir_processados(result.toPandas().to_dict(orient="records"))
    print(f"Silver: {contar('contratacoes_processadas')} documentos totais.")


def build_gold(data_interval_start, data_interval_end, **kwargs):
    from src.loading import buscar_processados_por_periodo, salvar_gold
    from src.processing import create_spark_session, build_gold

    di = data_interval_start.strftime("%Y%m%d")
    data_final = data_interval_end.strftime("%Y%m%d")
    periodo = f"{di}_{data_final}"

    registros = buscar_processados_por_periodo(di, data_final)
    if not registros:
        print("Nenhum registro na Silver para o período.")
        return

    spark = create_spark_session()
    agregacoes = build_gold(spark, registros, periodo)

    chaves = {
        "gold_area_de_servico": ["periodo", "uf", "ramo_mei"],
        "gold_estado":          ["periodo", "uf"],
        "gold_faixa_de_valor":  ["periodo", "uf", "faixa_valor"],
        "gold_situacao":        ["periodo", "uf", "situacao_nome"],
        "gold_por_mes":         ["uf", "ano", "mes"],
    }
    for colecao, registros_gold in agregacoes.items():
        salvar_gold(colecao, registros_gold, chaves[colecao])


with DAG(
    dag_id="pncp_pipeline",
    description="Coleta e processa contratações do PNCP: Bronze → Silver → Gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "pncp",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pncp"],
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_bronze",
        python_callable=ingest_bronze,
    )

    t_transform = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
    )

    t_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold,
    )

    t_ingest >> t_transform >> t_gold
