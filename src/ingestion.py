import time
import requests
from src.config import BASE_URL, DEFAULT_PARAMS


def _build_params(data_inicial: str, data_final: str, modalidade: int, uf: str, pagina: int, tamanho: int) -> dict:
    return {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "uf": uf.upper(),
        "pagina": pagina,
        "tamanhoPagina": tamanho,
    }


def fetch_page(data_inicial: str, data_final: str, modalidade: int, uf: str, pagina: int, tamanho: int, tentativas: int = 3) -> dict:
    """Busca uma unica pagina da API PNCP. Tenta ate 3 vezes antes de desistir."""
    params = _build_params(data_inicial, data_final, modalidade, uf, pagina, tamanho)
    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            print(f"  [ERRO HTTP {status}] pagina {pagina}: {e}")
            print(f"  [DETALHE] {response.text[:200]}")
            if status < 500:
                return {}
            if tentativa < tentativas:
                espera = 10 * tentativa
                print(f"  [TENTATIVA {tentativa}/{tentativas}] aguardando {espera}s...")
                time.sleep(espera)
        except requests.exceptions.RequestException as e:
            print(f"  [TENTATIVA {tentativa}/{tentativas}] pagina {pagina}: {e}")
            if tentativa < tentativas:
                time.sleep(5 * tentativa)
    print(f"  [FALHA] pagina {pagina} nao respondeu apos {tentativas} tentativas.")
    return {}


def fetch_all_pages(
    data_inicial: str,
    data_final: str,
    modalidade: int = DEFAULT_PARAMS["codigoModalidadeContratacao"],
    uf: str = DEFAULT_PARAMS["uf"],
    tamanho: int = DEFAULT_PARAMS["tamanhoPagina"],
    delay_segundos: float = 1.5,
    max_paginas: int | None = None,
) -> list[dict]:
    """
    Percorre todas as paginas da API e retorna lista com todos os registros.

    delay_segundos: pausa entre requisicoes para respeitar o rate limit da API.
    """
    print(f"Iniciando coleta | periodo: {data_inicial} -> {data_final} | UF: {uf} | modalidade: {modalidade}")

    # primeira página p descobrir o total
    primeira_pagina = fetch_page(data_inicial, data_final, modalidade, uf, 1, tamanho)
    if not primeira_pagina:
        print("Nenhum dado retornado na primeira pagina.")
        return []

    total_paginas = primeira_pagina.get("totalPaginas", 1)
    total_registros = primeira_pagina.get("totalRegistros", 0)
    if max_paginas:
        total_paginas = min(total_paginas, max_paginas)
    print(f"Total de registros: {total_registros} | Paginas a coletar: {total_paginas}")

    todos_registros = primeira_pagina.get("data", [])

    for pagina in range(2, total_paginas + 1):
        print(f"  Coletando pagina {pagina}/{total_paginas}...", end="\r")
        time.sleep(delay_segundos)
        resultado = fetch_page(data_inicial, data_final, modalidade, uf, pagina, tamanho)
        registros = resultado.get("data", [])
        todos_registros.extend(registros)

    print(f"\nColeta concluida: {len(todos_registros)} registros obtidos.")
    return todos_registros


