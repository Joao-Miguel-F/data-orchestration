"""
Servidor MCP para o pipeline PNCP (data-orchestration).
Expõe ferramentas para consultar dados do MongoDB via protocolo stdio.

Requisitos:
    pip install mcp pymongo certifi python-dotenv
"""

import os
import json
from datetime import datetime, timezone
from typing import Any

import certifi
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Tool,
    TextContent,
    CallToolResult,
)
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://larissa:engbigdata%40@cluster-bi-data.fzpirih.mongodb.net/?appName=cluster-bi-data",
)
MONGO_DB = os.getenv("MONGO_DB", "pncp")

_client: MongoClient | None = None


def _get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return _client[MONGO_DB]


def _fmt(data: str) -> str:
    """Converte YYYYMMDD → YYYY-MM-DD."""
    if len(data) == 8:
        return f"{data[:4]}-{data[4:6]}-{data[6:]}"
    return data


def _json_safe(obj: Any) -> Any:
    """Torna objetos MongoDB serializáveis em JSON."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items() if k != "_id"}
    if isinstance(obj, list):
        return [_json_safe(i) for i in obj]
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


# ---------------------------------------------------------------------------
# Servidor MCP
# ---------------------------------------------------------------------------
server = Server("pncp-mcp-server")


# ── Definição das ferramentas ────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="contar_registros",
            description=(
                "Conta o total de documentos em uma coleção do MongoDB do projeto PNCP. "
                "Coleções disponíveis: contratacoes_raw, contratacoes_processadas, "
                "gold_area_de_servico, gold_estado, gold_faixa_de_valor, gold_situacao, gold_por_mes."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "colecao": {
                        "type": "string",
                        "description": "Nome da coleção MongoDB a ser consultada.",
                    }
                },
                "required": ["colecao"],
            },
        ),
        Tool(
            name="resumo_por_estado",
            description=(
                "Retorna o resumo de contratações por estado (UF) para um período. "
                "Inclui total de contratações, valor total e órgãos distintos."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "periodo": {
                        "type": "string",
                        "description": "Período no formato YYYYMMDD_YYYYMMDD (ex: 20260101_20260131). "
                                       "Deixe vazio para retornar todos os períodos.",
                    },
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado (ex: SP, RJ). Deixe vazio para todos os estados.",
                    },
                    "limite": {
                        "type": "integer",
                        "description": "Número máximo de registros a retornar (padrão: 27).",
                        "default": 27,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="resumo_por_ramo_mei",
            description=(
                "Retorna o total de contratações e valor por ramo MEI (categoria de serviço): "
                "Obras, TI, Serviços, Compras, Outros. Pode filtrar por UF e período."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado (ex: PE, SP). Deixe vazio para todos.",
                    },
                    "periodo": {
                        "type": "string",
                        "description": "Período no formato YYYYMMDD_YYYYMMDD. Deixe vazio para todos.",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="resumo_por_faixa_valor",
            description=(
                "Retorna a distribuição de contratações por faixa de valor: "
                "Ate R$17.600 / R$17.601 a R$80.000 / R$80.001 a R$500.000 / Acima de R$500.000. "
                "Pode filtrar por UF e período."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado. Deixe vazio para todos.",
                    },
                    "periodo": {
                        "type": "string",
                        "description": "Período no formato YYYYMMDD_YYYYMMDD. Deixe vazio para todos.",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="resumo_por_situacao",
            description=(
                "Retorna a distribuição de contratações por situação (ex: Divulgada no PNCP, "
                "Cancelada, etc.). Pode filtrar por UF e período."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado. Deixe vazio para todos.",
                    },
                    "periodo": {
                        "type": "string",
                        "description": "Período no formato YYYYMMDD_YYYYMMDD. Deixe vazio para todos.",
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="evolucao_mensal",
            description=(
                "Retorna a evolução mensal de contratações por UF, com total, valor e órgãos distintos. "
                "Útil para analisar tendências ao longo do tempo."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado. Deixe vazio para todos.",
                    },
                    "ano": {
                        "type": "integer",
                        "description": "Ano de referência (ex: 2026). Deixe vazio para todos.",
                    },
                    "limite": {
                        "type": "integer",
                        "description": "Número máximo de registros (padrão: 50).",
                        "default": 50,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="buscar_contratacoes",
            description=(
                "Busca contratações específicas na camada Silver (processada) do pipeline. "
                "Permite filtrar por UF, período de publicação e busca por texto no objeto da compra."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado (ex: PE, SP).",
                    },
                    "data_inicial": {
                        "type": "string",
                        "description": "Data inicial no formato YYYYMMDD (ex: 20260101).",
                    },
                    "data_final": {
                        "type": "string",
                        "description": "Data final no formato YYYYMMDD (ex: 20260131).",
                    },
                    "texto_objeto": {
                        "type": "string",
                        "description": "Texto para buscar no campo objeto_compra (busca parcial, case-insensitive).",
                    },
                    "ramo_mei": {
                        "type": "string",
                        "description": "Filtrar por ramo MEI: Obras, TI, Serviços, Compras, Outros.",
                    },
                    "limite": {
                        "type": "integer",
                        "description": "Número máximo de registros (padrão: 10, máximo: 50).",
                        "default": 10,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="top_orgaos_contratantes",
            description=(
                "Retorna os órgãos/entidades que mais realizaram contratações por dispensa, "
                "com total de contratos e valor total. Pode filtrar por UF."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uf": {
                        "type": "string",
                        "description": "Sigla do estado. Deixe vazio para todos.",
                    },
                    "limite": {
                        "type": "integer",
                        "description": "Top N órgãos a retornar (padrão: 10).",
                        "default": 10,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="estatisticas_gerais",
            description=(
                "Retorna um painel com estatísticas gerais do pipeline: total de registros em cada "
                "camada (raw, silver, gold), maior valor de contratação e data da última atualização."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
    ]


# ── Implementação das ferramentas ────────────────────────────────────────────

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> CallToolResult:
    try:
        result = _dispatch(name, arguments)
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]
        )
    except Exception as exc:
        return CallToolResult(
            content=[TextContent(type="text", text=f"Erro ao executar '{name}': {exc}")]
        )


def _dispatch(name: str, args: dict) -> Any:
    db = _get_db()

    # ── contar_registros ─────────────────────────────────────────────────────
    if name == "contar_registros":
        colecao = args["colecao"]
        total = db[colecao].count_documents({})
        return {"colecao": colecao, "total": total}

    # ── resumo_por_estado ────────────────────────────────────────────────────
    if name == "resumo_por_estado":
        filtro = {}
        if args.get("periodo"):
            filtro["periodo"] = args["periodo"]
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        limite = int(args.get("limite", 27))
        docs = list(
            db["gold_estado"]
            .find(filtro, {"_id": 0})
            .sort("total_contratacoes", -1)
            .limit(limite)
        )
        return {"total_estados": len(docs), "dados": _json_safe(docs)}

    # ── resumo_por_ramo_mei ──────────────────────────────────────────────────
    if name == "resumo_por_ramo_mei":
        filtro = {}
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        if args.get("periodo"):
            filtro["periodo"] = args["periodo"]
        pipeline = [
            {"$match": filtro},
            {
                "$group": {
                    "_id": "$ramo_mei",
                    "total_contratacoes": {"$sum": "$total_contratacoes"},
                    "valor_total": {"$sum": "$valor_total"},
                }
            },
            {"$sort": {"total_contratacoes": -1}},
        ]
        docs = list(db["gold_area_de_servico"].aggregate(pipeline))
        return {
            "dados": [
                {
                    "ramo_mei": d["_id"],
                    "total_contratacoes": d["total_contratacoes"],
                    "valor_total": round(d["valor_total"], 2),
                }
                for d in docs
            ]
        }

    # ── resumo_por_faixa_valor ───────────────────────────────────────────────
    if name == "resumo_por_faixa_valor":
        filtro = {}
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        if args.get("periodo"):
            filtro["periodo"] = args["periodo"]
        pipeline = [
            {"$match": filtro},
            {
                "$group": {
                    "_id": "$faixa_valor",
                    "total_contratacoes": {"$sum": "$total_contratacoes"},
                    "valor_total": {"$sum": "$valor_total"},
                }
            },
            {"$sort": {"total_contratacoes": -1}},
        ]
        docs = list(db["gold_faixa_de_valor"].aggregate(pipeline))
        return {
            "dados": [
                {
                    "faixa_valor": d["_id"],
                    "total_contratacoes": d["total_contratacoes"],
                    "valor_total": round(d["valor_total"], 2),
                }
                for d in docs
            ]
        }

    # ── resumo_por_situacao ──────────────────────────────────────────────────
    if name == "resumo_por_situacao":
        filtro = {}
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        if args.get("periodo"):
            filtro["periodo"] = args["periodo"]
        pipeline = [
            {"$match": filtro},
            {
                "$group": {
                    "_id": "$situacao_nome",
                    "total_contratacoes": {"$sum": "$total_contratacoes"},
                    "valor_total": {"$sum": "$valor_total"},
                }
            },
            {"$sort": {"total_contratacoes": -1}},
        ]
        docs = list(db["gold_situacao"].aggregate(pipeline))
        return {
            "dados": [
                {
                    "situacao": d["_id"],
                    "total_contratacoes": d["total_contratacoes"],
                    "valor_total": round(d["valor_total"], 2),
                }
                for d in docs
            ]
        }

    # ── evolucao_mensal ──────────────────────────────────────────────────────
    if name == "evolucao_mensal":
        filtro = {}
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        if args.get("ano"):
            filtro["ano"] = int(args["ano"])
        limite = int(args.get("limite", 50))
        docs = list(
            db["gold_por_mes"]
            .find(filtro, {"_id": 0})
            .sort([("ano", 1), ("mes", 1)])
            .limit(limite)
        )
        return {"total_registros": len(docs), "dados": _json_safe(docs)}

    # ── buscar_contratacoes ──────────────────────────────────────────────────
    if name == "buscar_contratacoes":
        filtro = {}
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        if args.get("data_inicial") and args.get("data_final"):
            filtro["data_publicacao_pncp"] = {
                "$gte": _fmt(args["data_inicial"]),
                "$lte": _fmt(args["data_final"]) + "T23:59:59",
            }
        if args.get("texto_objeto"):
            filtro["objeto_compra"] = {"$regex": args["texto_objeto"], "$options": "i"}
        if args.get("ramo_mei"):
            filtro["ramo_mei"] = args["ramo_mei"]
        limite = min(int(args.get("limite", 10)), 50)
        docs = list(
            db["contratacoes_processadas"]
            .find(filtro, {"_id": 0})
            .limit(limite)
        )
        return {"total_retornado": len(docs), "contratacoes": _json_safe(docs)}

    # ── top_orgaos_contratantes ──────────────────────────────────────────────
    if name == "top_orgaos_contratantes":
        filtro = {}
        if args.get("uf"):
            filtro["uf"] = args["uf"].upper()
        limite = int(args.get("limite", 10))
        pipeline = [
            {"$match": filtro},
            {
                "$group": {
                    "_id": {
                        "cnpj": "$cnpj_orgao",
                        "razao_social": "$razao_social",
                        "uf": "$uf",
                    },
                    "total_contratacoes": {"$sum": 1},
                    "valor_total": {"$sum": "$valor_total_estimado"},
                }
            },
            {"$sort": {"total_contratacoes": -1}},
            {"$limit": limite},
        ]
        docs = list(db["contratacoes_processadas"].aggregate(pipeline))
        return {
            "dados": [
                {
                    "razao_social": d["_id"].get("razao_social", "N/A"),
                    "cnpj": d["_id"].get("cnpj", "N/A"),
                    "uf": d["_id"].get("uf", "N/A"),
                    "total_contratacoes": d["total_contratacoes"],
                    "valor_total": round(d.get("valor_total") or 0, 2),
                }
                for d in docs
            ]
        }

    # ── estatisticas_gerais ──────────────────────────────────────────────────
    if name == "estatisticas_gerais":
        raw = db["contratacoes_raw"].count_documents({})
        silver = db["contratacoes_processadas"].count_documents({})
        gold_colecoes = {
            "gold_area_de_servico": db["gold_area_de_servico"].count_documents({}),
            "gold_estado": db["gold_estado"].count_documents({}),
            "gold_faixa_de_valor": db["gold_faixa_de_valor"].count_documents({}),
            "gold_situacao": db["gold_situacao"].count_documents({}),
            "gold_por_mes": db["gold_por_mes"].count_documents({}),
        }
        # Maior valor
        top = list(
            db["contratacoes_processadas"]
            .find({}, {"_id": 0, "valor_total_estimado": 1, "objeto_compra": 1, "uf": 1})
            .sort("valor_total_estimado", -1)
            .limit(1)
        )
        ultima = db["contratacoes_processadas"].find_one(
            {}, {"_id": 0, "data_coleta": 1}, sort=[("data_coleta", -1)]
        )
        return {
            "camada_bronze_raw": raw,
            "camada_silver_processadas": silver,
            "camada_gold": gold_colecoes,
            "maior_contratacao": _json_safe(top[0]) if top else None,
            "ultima_atualizacao": _json_safe(ultima.get("data_coleta")) if ultima else None,
            "timestamp_consulta": datetime.now(timezone.utc).isoformat(),
        }

    return {"erro": f"Ferramenta desconhecida: {name}"}


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
