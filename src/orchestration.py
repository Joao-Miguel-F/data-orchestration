"""
Pipeline PNCP orquestrado com Prefect.

Cada etapa do pipeline é uma @task monitorada individualmente.
O @flow coordena a execução e registra tudo no Prefect.

Fluxo (conforme requisito):
  API PNCP -> MongoDB raw -> Spark (flatten, classifica ramo MEI, deduplica) -> MongoDB processados

Resiliência na coleta:
  - Erros 5xx (servidor PNCP instável): retry com backoff (10s, 20s)
  - Erros 4xx: falha imediata (erro do cliente, retry não resolve)
  - Erros de rede: retry com backoff (5s, 10s)
  - A task_coleta tem até 3 retries automáticos pelo Prefect
"""

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger

from src.ingestion import fetch_all_pages
from src.database import inserir_raw, inserir_processados, contar, buscar_raw_por_periodo
from src.processing import (
    create_spark_session,
    load_from_records,
    flatten_and_select,
    classify_ramo_mei,
    deduplicate,
    add_data_coleta,
    show_summary,
)


@task(name="Coleta API PNCP", retries=3, retry_delay_seconds=10, log_prints=True)
def task_coleta(data_inicial: str, data_final: str, modalidade: int, uf: str, tamanho: int) -> None:
    """Busca todos os registros da API e persiste no MongoDB raw."""
    logger = get_run_logger()
    logger.info(f"Iniciando coleta | {data_inicial} -> {data_final} | UF: {uf} | modalidade: {modalidade}")

    registros = fetch_all_pages(
        data_inicial=data_inicial,
        data_final=data_final,
        modalidade=modalidade,
        uf=uf,
        tamanho=tamanho,
    )

    if not registros:
        raise ValueError("Nenhum registro retornado pela API.")

    logger.info(f"{len(registros)} registros coletados.")
    inserir_raw(registros)
    logger.info(f"MongoDB raw: {contar('contratacoes_raw')} documentos no total.")


@task(name="Processamento Spark", log_prints=True, cache_policy=NO_CACHE)
def task_processamento(data_inicial: str, data_final: str, uf: str, exibir_resumo: bool) -> object:
    """Le do MongoDB raw, transforma com Spark e retorna o DataFrame processado."""
    logger = get_run_logger()

    registros = buscar_raw_por_periodo(data_inicial, data_final, uf)
    if not registros:
        raise ValueError("Nenhum registro encontrado no MongoDB para o periodo informado.")
    logger.info(f"Lidos {len(registros)} registros do MongoDB raw.")

    spark = create_spark_session()
    df_raw = load_from_records(spark, registros)

    df = (
        df_raw
        .transform(flatten_and_select)
        .transform(classify_ramo_mei)
        .transform(deduplicate)
        .transform(add_data_coleta)
    )

    total = df.count()
    logger.info(f"DataFrame processado: {total} registros | colunas: {df.columns}")

    if exibir_resumo:
        show_summary(df)

    return df


@task(name="Salvar MongoDB", log_prints=True, cache_policy=NO_CACHE)
def task_salvar(df: object) -> int:
    """Persiste o DataFrame processado no MongoDB."""
    logger = get_run_logger()

    registros = df.toPandas().to_dict(orient="records")
    inserir_processados(registros)
    total = contar('contratacoes_processadas')
    logger.info(f"MongoDB processados: {total} documentos no total.")

    return total


@flow(name="Pipeline PNCP", log_prints=True)
def run_pipeline(
    data_inicial: str,
    data_final: str,
    modalidade: int = 8,
    uf: str = "PE",
    tamanho_pagina: int = 20,
    exibir_resumo: bool = True,
) -> None:
    """
    Flow principal — coordena coleta, processamento e persistencia.

    Args:
        data_inicial: formato AAAAMMDD (ex: '20260401')
        data_final:   formato AAAAMMDD (ex: '20260430')
        modalidade:   codigo da modalidade (padrao 8 = Dispensa)
        uf:           sigla do estado (padrao 'PE')
        tamanho_pagina: registros por pagina da API (max 500)
        exibir_resumo: imprime estatisticas basicas no console
    """
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info(f"PIPELINE PNCP | {data_inicial} -> {data_final} | UF: {uf}")
    logger.info("=" * 50)

    task_coleta(data_inicial, data_final, modalidade, uf, tamanho_pagina)
    df = task_processamento(data_inicial, data_final, uf, exibir_resumo)
    task_salvar(df)

    logger.info("Pipeline concluido.")
