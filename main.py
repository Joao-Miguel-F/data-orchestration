"""
Entrada do pipeline de orquestracao PNCP.

Uso rápido:
    python main.py --data-inicial 20260401 --data-final 20260430

Uso completo:
    python main.py --data-inicial 20260401 --data-final 20260430 \
        --uf PE --modalidade 8 --tamanho-pagina 50
"""

import argparse
from src.orchestration import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline de orquestracao de dados da API PNCP"
    )
    parser.add_argument(
        "--data-inicial",
        required=True,
        help="Data inicial no formato AAAAMMDD (ex: 20260401)",
    )
    parser.add_argument(
        "--data-final",
        required=True,
        help="Data final no formato AAAAMMDD (ex: 20260430)",
    )
    parser.add_argument(
        "--uf",
        default="PE",
        help="Sigla do estado",
    )
    parser.add_argument(
        "--modalidade",
        type=int,
        default=8,
        help="Codigo da modalidade de contratacao (8 = Dispensa)",
    )
    parser.add_argument(
        "--tamanho-pagina",
        type=int,
        default=20,
        help="Registros por pagina da API (padrao: 20, max: 500)",
    )
    parser.add_argument(
        "--sem-resumo",
        action="store_true",
        help="Suprime o resumo estatistico no console",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        modalidade=args.modalidade,
        uf=args.uf,
        tamanho_pagina=args.tamanho_pagina,
        exibir_resumo=not args.sem_resumo,
    )
