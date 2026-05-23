import time
import requests
from src.config import BASE_URL, DEFAULT_PARAMS

TIMEOUT = (10, 15)


def _build_params(data_inicial: str, data_final: str, modalidade: int, uf: str | None, pagina: int, tamanho: int) -> dict:
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "pagina": pagina,
        "tamanhoPagina": tamanho,
    }
    if uf:
        params["uf"] = uf.upper()
    return params


def _fetch_page(
    session: requests.Session,
    data_inicial: str,
    data_final: str,
    modalidade: int,
    uf: str | None,
    pagina: int,
    tamanho: int,
    tentativas: int = 3,
) -> dict:
    params = _build_params(data_inicial, data_final, modalidade, uf, pagina, tamanho)
    for tentativa in range(1, tentativas + 1):
        try:
            response = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                # Resposta vazia — sem dados para este periodo/UF, nao adianta tentar de novo
                return {}
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"  [TIMEOUT/REDE] pagina {pagina} (tentativa {tentativa}/{tentativas}): {e}")
            if tentativa < tentativas:
                espera = 10 * tentativa
                print(f"  aguardando {espera}s...")
                time.sleep(espera)
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status < 500:
                return {}
            print(f"  [ERRO HTTP {status}] pagina {pagina}: {e}")
            if tentativa < tentativas:
                time.sleep(10 * tentativa)
        except requests.exceptions.RequestException as e:
            print(f"  [ERRO REDE] pagina {pagina} (tentativa {tentativa}/{tentativas}): {e}")
            if tentativa < tentativas:
                time.sleep(5 * tentativa)
    return {}


def iter_pages(
    data_inicial: str,
    data_final: str,
    modalidade: int = DEFAULT_PARAMS["codigoModalidadeContratacao"],
    uf: str | None = None,
    tamanho: int = DEFAULT_PARAMS["tamanhoPagina"],
    delay_segundos: float = 1.5,
):
    session = requests.Session()

    primeira = _fetch_page(session, data_inicial, data_final, modalidade, uf, 1, tamanho)
    if not primeira:
        return

    total_paginas = primeira.get("totalPaginas", 1)
    total_registros = primeira.get("totalRegistros", 0)
    print(f"Total de registros: {total_registros} | Total de paginas: {total_paginas}")

    yield 1, total_paginas, primeira.get("data", [])

    for pagina in range(2, total_paginas + 1):
        time.sleep(delay_segundos)
        resultado = _fetch_page(session, data_inicial, data_final, modalidade, uf, pagina, tamanho)
        yield pagina, total_paginas, resultado.get("data", [])
