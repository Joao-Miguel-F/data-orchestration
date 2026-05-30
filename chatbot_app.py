"""
Chatbot PNCP — Interface Streamlit
Arquitetura limpa: MCP síncrono via JSON-RPC sobre subprocess stdio.
Sem asyncio no Streamlit. Sem ThreadPoolExecutor. Sem run_until_complete.
"""

import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import streamlit as st
from ollama import Client as OllamaClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pncp_chatbot")

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

MONGO_URI = os.getenv(
    "MONGO_URI",
    "coloque aqui sua URI",
)
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))
MCP_SERVER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mcp_server.py")
MAX_AGENT_ITERATIONS = 10

SYSTEM_PROMPT = """Você é o assistente PNCP, especialista em análise de dados de contratações públicas por dispensa de licitação no Brasil.

Você tem acesso a ferramentas que consultam um banco de dados MongoDB com dados reais do Portal Nacional de Contratações Públicas (PNCP), processados por um pipeline ETL em três camadas: Bronze (raw), Silver (processada) e Gold (agregada).

**Ferramentas disponíveis:**
- `contar_registros` — conta documentos em qualquer coleção
- `resumo_por_estado` — contratações agrupadas por UF
- `resumo_por_ramo_mei` — distribuição por ramo (Obras, TI, Serviços, Compras, Outros)
- `resumo_por_faixa_valor` — distribuição por faixa de valor
- `resumo_por_situacao` — distribuição por situação da contratação
- `evolucao_mensal` — série temporal mensal por UF
- `buscar_contratacoes` — busca contratos específicos com filtros
- `top_orgaos_contratantes` — ranking de órgãos que mais contratam
- `estatisticas_gerais` — painel geral com totais de todas as camadas

**Instruções:**
1. Sempre use as ferramentas para obter dados reais antes de responder perguntas quantitativas.
2. Formate valores monetários em R$ com separadores de milhar.
3. Use emojis para tornar as respostas mais visuais (📊 📈 💰 🏛️ 🗺️).
4. Quando não souber algo, diga que precisa consultar os dados e use a ferramenta adequada.
5. Responda sempre em português do Brasil.
6. Se os dados estiverem vazios, explique que o pipeline ainda não ingestou dados para esse período.
"""

# ---------------------------------------------------------------------------
# MCP Client — JSON-RPC síncrono sobre subprocess stdio
# ---------------------------------------------------------------------------

@dataclass
class MCPTool:
    name: str
    description: str
    input_schema: dict

    @property
    def inputSchema(self):
        return self.input_schema


class MCPClient:
    """
    Cliente MCP síncrono. Mantém um único subprocesso vivo durante a sessão.
    Comunicação via JSON-RPC 2.0 sobre stdin/stdout do servidor MCP.
    Sem asyncio. Sem threads extras.
    """

    def __init__(self, server_path: str, mongo_uri: str):
        self.server_path = server_path
        self.mongo_uri = mongo_uri
        self._proc: subprocess.Popen | None = None
        self._lock = threading.Lock()
        self._req_id = 0
        self._initialized = False
        self.tools: list[MCPTool] = []
        self.error: str | None = None

    # ------------------------------------------------------------------ #
    # Controle do subprocesso                                              #
    # ------------------------------------------------------------------ #

    def _start(self) -> bool:
        """Inicia o subprocesso MCP se ainda não estiver rodando."""
        if self._proc and self._proc.poll() is None:
            return True
        log.info("Iniciando subprocesso MCP: %s", self.server_path)
        env = {**os.environ, "MONGO_URI": self.mongo_uri}
        try:
            self._proc = subprocess.Popen(
                [sys.executable, self.server_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                text=True,
                bufsize=1,
            )
            self._initialized = False
            return True
        except Exception as exc:
            log.error("Falha ao iniciar subprocesso MCP: %s", exc)
            self.error = str(exc)
            return False

    def _stop(self):
        """Encerra o subprocesso MCP de forma limpa."""
        if self._proc:
            try:
                self._proc.stdin.close()
                self._proc.terminate()
                self._proc.wait(timeout=5)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None
            self._initialized = False
        log.info("Subprocesso MCP encerrado.")

    def _alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    # ------------------------------------------------------------------ #
    # Comunicação JSON-RPC                                                 #
    # ------------------------------------------------------------------ #

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _send(self, payload: dict, timeout: float = 30.0) -> dict:
        """Envia um request JSON-RPC e lê a resposta. Thread-safe."""
        with self._lock:
            if not self._alive():
                raise RuntimeError("Subprocesso MCP não está rodando.")

            line = json.dumps(payload) + "\n"
            log.debug("MCP → %s", line.strip())

            try:
                self._proc.stdin.write(line)
                self._proc.stdin.flush()
            except BrokenPipeError as exc:
                raise RuntimeError(f"Pipe quebrado ao escrever para MCP: {exc}") from exc

            # Lê linhas até encontrar uma com JSON válido contendo o mesmo id
            deadline = time.monotonic() + timeout
            req_id = payload.get("id")
            while time.monotonic() < deadline:
                if self._proc.stdout is None:
                    raise RuntimeError("stdout do MCP é None.")
                try:
                    self._proc.stdout.fileno()  # verifica se ainda aberto
                except Exception:
                    raise RuntimeError("stdout do MCP foi fechado.")

                raw = self._proc.stdout.readline()
                if not raw:
                    time.sleep(0.05)
                    continue
                raw = raw.strip()
                if not raw:
                    continue
                log.debug("MCP ← %s", raw[:300])
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Notificações (sem "id") são ignoradas
                if "id" not in data:
                    continue
                if req_id is not None and data["id"] != req_id:
                    continue
                return data

            raise TimeoutError(f"MCP não respondeu em {timeout}s (req_id={req_id})")

    # ------------------------------------------------------------------ #
    # Protocolo MCP                                                        #
    # ------------------------------------------------------------------ #

    def _initialize(self) -> bool:
        """Envia initialize + initialized ao servidor MCP."""
        if self._initialized:
            return True
        log.info("Inicializando sessão MCP...")
        init_req = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "pncp-chatbot", "version": "1.0"},
            },
        }
        try:
            resp = self._send(init_req, timeout=30)
        except Exception as exc:
            log.error("Falha no initialize MCP: %s", exc)
            self.error = str(exc)
            return False

        if "error" in resp:
            log.error("MCP initialize error: %s", resp["error"])
            self.error = str(resp["error"])
            return False

        # Notifica que o cliente está pronto (sem esperar resposta)
        notif = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
        try:
            with self._lock:
                self._proc.stdin.write(json.dumps(notif) + "\n")
                self._proc.stdin.flush()
        except Exception as exc:
            log.warning("Falha ao enviar notifications/initialized: %s", exc)

        self._initialized = True
        log.info("Sessão MCP inicializada com sucesso.")
        return True

    def conectar(self) -> bool:
        """Inicia subprocesso e inicializa sessão MCP."""
        self.error = None
        if not _file_exists(self.server_path):
            self.error = f"mcp_server.py não encontrado em: {self.server_path}"
            log.error(self.error)
            return False
        if not self._start():
            return False
        time.sleep(0.3)  # aguarda o servidor subir
        return self._initialize()

    def listar_ferramentas(self) -> list[MCPTool]:
        """Lista ferramentas disponíveis no servidor MCP."""
        if not self._initialized:
            raise RuntimeError("MCP não inicializado. Chame conectar() primeiro.")
        req = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/list",
            "params": {},
        }
        resp = self._send(req, timeout=30)
        if "error" in resp:
            raise RuntimeError(f"tools/list erro: {resp['error']}")
        raw_tools = resp.get("result", {}).get("tools", [])
        tools = []
        for t in raw_tools:
            tools.append(MCPTool(
                name=t.get("name", ""),
                description=t.get("description", ""),
                input_schema=t.get("inputSchema", {}),
            ))
        log.info("MCP: %d ferramentas carregadas.", len(tools))
        return tools

    def executar_ferramenta(self, name: str, arguments: dict, timeout: float = 60.0) -> str:
        """Executa uma ferramenta MCP e retorna o resultado como string."""
        if not self._initialized:
            raise RuntimeError("MCP não inicializado.")
        log.info("MCP executar_ferramenta: %s args=%s", name, arguments)
        req = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        resp = self._send(req, timeout=timeout)
        if "error" in resp:
            raise RuntimeError(f"tools/call erro: {resp['error']}")
        content = resp.get("result", {}).get("content", [])
        texts = [c.get("text", "") for c in content if c.get("type") == "text"]
        result = "\n".join(texts) if texts else "{}"
        log.info("MCP resultado de %s: %s…", name, result[:200])
        return result

    def __del__(self):
        self._stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _file_exists(path: str) -> bool:
    return os.path.isfile(path)


def _parse_arguments(raw: Any) -> dict:
    """Converte argumentos de tool_call para dict, independente do formato."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        try:
            result = json.loads(raw)
            return result if isinstance(result, dict) else {}
        except json.JSONDecodeError:
            log.warning("Não foi possível fazer parse dos argumentos: %s", raw[:200])
            return {}
    return {}


def _tools_to_ollama_format(tools: list[MCPTool]) -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.input_schema,
            },
        }
        for t in tools
    ]


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def check_ollama() -> tuple[bool, list[str], bool]:
    """Retorna (conectado, modelos_disponíveis, modelo_correto_presente)."""
    try:
        client = OllamaClient(host=OLLAMA_HOST)
        models_resp = client.list()
        model_names = [m.model for m in models_resp.models]
        has_model = any(OLLAMA_MODEL in name for name in model_names)
        log.info("Ollama OK. Modelos: %s", model_names)
        return True, model_names, has_model
    except Exception as exc:
        log.warning("Ollama check falhou: %s", exc)
        return False, [], False


def check_mongodb() -> tuple[bool, str]:
    """Verifica conectividade com MongoDB Atlas."""
    try:
        from pymongo import MongoClient
        import certifi
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
        client.admin.command("ping")
        client.close()
        log.info("MongoDB OK.")
        return True, "Conectado"
    except Exception as exc:
        log.warning("MongoDB check falhou: %s", exc)
        return False, str(exc)[:120]


# ---------------------------------------------------------------------------
# Loop agentico
# ---------------------------------------------------------------------------

def agent_respond(
    user_message: str,
    history: list[dict],
    mcp_client: MCPClient,
    tools: list[MCPTool],
    status_placeholder=None,
) -> tuple[str, list[dict]]:
    """
    Loop agentico robusto:
    1. Envia mensagem ao Ollama.
    2. Se houver tool_calls, executa via MCP e reinjeta resultados.
    3. Repete até resposta final ou MAX_AGENT_ITERATIONS.
    Retorna (resposta_final, log_de_tool_calls).
    """
    log.info("Agente iniciado. user_message='%s'", user_message[:100])

    ollama_client = OllamaClient(host=OLLAMA_HOST)
    ollama_tools = _tools_to_ollama_format(tools)

    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in history:
        if m["role"] in ("user", "assistant"):
            messages.append({"role": m["role"], "content": m["content"]})
    messages.append({"role": "user", "content": user_message})

    tool_calls_log: list[dict] = []

    def _update_status(msg: str):
        if status_placeholder:
            status_placeholder.markdown(msg)
        log.info("Status: %s", msg)

    for iteration in range(MAX_AGENT_ITERATIONS):
        log.info("Iteração %d/%d — chamando Ollama…", iteration + 1, MAX_AGENT_ITERATIONS)
        _update_status(f"🤔 Pensando… (iteração {iteration + 1})")

        try:
            response = ollama_client.chat(
                model=OLLAMA_MODEL,
                messages=messages,
                tools=ollama_tools if tools else None,
                options={"num_predict": 4096},
            )
        except Exception as exc:
            log.error("Erro Ollama na iteração %d: %s", iteration + 1, exc)
            return (
                f"❌ Erro ao comunicar com Ollama: {exc}\n\n"
                f"Verifique se o serviço está rodando em `{OLLAMA_HOST}` "
                f"e se o modelo `{OLLAMA_MODEL}` foi baixado.",
                tool_calls_log,
            )

        msg = response.message
        raw_tool_calls = msg.tool_calls or []

        # Sem tool_calls → resposta final
        if not raw_tool_calls:
            final = (msg.content or "").strip()
            log.info("Resposta final obtida (%d chars).", len(final))
            _update_status("")
            return final, tool_calls_log

        log.info("%d tool_call(s) detectado(s).", len(raw_tool_calls))

        # Adiciona mensagem do assistente com tool_calls ao histórico
        assistant_msg: dict[str, Any] = {
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": [],
        }
        for tc in raw_tool_calls:
            assistant_msg["tool_calls"].append({
                "function": {
                    "name": tc.function.name,
                    "arguments": _parse_arguments(tc.function.arguments),
                }
            })
        messages.append(assistant_msg)

        # Executa cada ferramenta e injeta resultado
        for tc in raw_tool_calls:
            tool_name = tc.function.name
            arguments = _parse_arguments(tc.function.arguments)

            log.info("Executando ferramenta '%s' com args: %s", tool_name, arguments)
            _update_status(f"🔧 Consultando `{tool_name}`…")

            try:
                result = mcp_client.executar_ferramenta(tool_name, arguments)
            except Exception as exc:
                log.error("Erro ao executar ferramenta '%s': %s", tool_name, exc)
                result = json.dumps({"error": str(exc)})

            preview = result[:400] + "…" if len(result) > 400 else result
            tool_calls_log.append({
                "tool": tool_name,
                "args": arguments,
                "result_preview": preview,
            })

            messages.append({
                "role": "tool",
                "content": result,
            })

    log.warning("Número máximo de iterações (%d) atingido.", MAX_AGENT_ITERATIONS)
    _update_status("")
    return "⚠️ Atingido o número máximo de iterações do agente.", tool_calls_log


# ---------------------------------------------------------------------------
# Inicialização de sessão
# ---------------------------------------------------------------------------

def _init_session():
    """Inicializa session_state na primeira execução."""
    defaults = {
        "messages": [],
        "mcp_client": None,
        "mcp_tools": [],
        "mcp_ok": False,
        "mcp_error": None,
        "ollama_ok": False,
        "ollama_models": [],
        "ollama_has_model": False,
        "mongo_ok": False,
        "mongo_error": None,
        "system_checked": False,
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val


def _run_system_checks():
    """Executa health checks e inicializa MCPClient. Chamado uma vez por sessão."""
    if st.session_state.system_checked:
        return

    log.info("Executando health checks do sistema…")

    # Ollama
    ok, models, has_model = check_ollama()
    st.session_state.ollama_ok = ok
    st.session_state.ollama_models = models
    st.session_state.ollama_has_model = has_model

    # MongoDB
    mongo_ok, mongo_msg = check_mongodb()
    st.session_state.mongo_ok = mongo_ok
    st.session_state.mongo_error = mongo_msg

    # MCP
    client = MCPClient(server_path=MCP_SERVER_PATH, mongo_uri=MONGO_URI)
    if client.conectar():
        try:
            tools = client.listar_ferramentas()
            st.session_state.mcp_client = client
            st.session_state.mcp_tools = tools
            st.session_state.mcp_ok = True
            st.session_state.mcp_error = None
            log.info("MCPClient pronto com %d ferramentas.", len(tools))
        except Exception as exc:
            st.session_state.mcp_ok = False
            st.session_state.mcp_error = str(exc)
            log.error("Falha ao listar ferramentas MCP: %s", exc)
    else:
        st.session_state.mcp_ok = False
        st.session_state.mcp_error = client.error or "Falha desconhecida ao conectar ao MCP."
        log.error("MCPClient falhou ao conectar: %s", st.session_state.mcp_error)

    st.session_state.system_checked = True


# ---------------------------------------------------------------------------
# Interface Streamlit
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="PNCP Chatbot",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;800&family=IBM+Plex+Mono:wght@400;600&display=swap');

    :root {
        --bg: #0d1117;
        --surface: #161b22;
        --border: #30363d;
        --accent: #00e5ff;
        --accent2: #7c3aed;
        --text: #e6edf3;
        --text-muted: #8b949e;
        --green: #3fb950;
        --red: #f85149;
        --yellow: #d29922;
    }

    html, body, [class*="css"] {
        font-family: 'Syne', sans-serif;
        background-color: var(--bg);
        color: var(--text);
    }

    .stApp { background-color: var(--bg); }

    .main-header {
        background: linear-gradient(135deg, #0d1117 0%, #161b22 50%, #0d1117 100%);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 24px 32px;
        margin-bottom: 24px;
        position: relative;
        overflow: hidden;
    }
    .main-header::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, var(--accent2), var(--accent), var(--accent2));
    }
    .main-header h1 { font-size: 2rem; font-weight: 800; color: var(--text); margin: 0; letter-spacing: -0.5px; }
    .main-header p { color: var(--text-muted); margin: 4px 0 0 0; font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; }

    .sidebar-card {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 12px 14px;
        margin-bottom: 10px;
        font-size: 0.85rem;
        line-height: 1.6;
    }
    .sidebar-card.ok   { border-left: 3px solid var(--green); }
    .sidebar-card.warn { border-left: 3px solid var(--yellow); }
    .sidebar-card.err  { border-left: 3px solid var(--red); }

    .stButton > button {
        background: var(--surface);
        border: 1px solid var(--border);
        color: var(--text);
        border-radius: 8px;
        font-family: 'Syne', sans-serif;
        transition: all 0.2s;
    }
    .stButton > button:hover { border-color: var(--accent); color: var(--accent); }

    code {
        font-family: 'IBM Plex Mono', monospace;
        background: rgba(0,229,255,0.08);
        color: var(--accent);
        padding: 1px 5px;
        border-radius: 3px;
        font-size: 0.85em;
    }

    div[data-testid="stChatMessage"] { background: transparent !important; }

    .stChatInputContainer > div {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
    }
</style>
""", unsafe_allow_html=True)

# Inicialização
_init_session()

# Health checks (uma vez por sessão Streamlit)
with st.spinner("🔍 Verificando serviços…"):
    _run_system_checks()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### 🏛️ PNCP Chatbot")
    st.markdown("<hr style='border-color:#30363d; margin: 8px 0'>", unsafe_allow_html=True)
    st.markdown("**Status dos Serviços**")

    # Ollama
    if st.session_state.ollama_ok and st.session_state.ollama_has_model:
        st.markdown(
            f"<div class='sidebar-card ok'>✅ <b>Ollama</b> conectado<br>"
            f"<code>{OLLAMA_MODEL}</code></div>",
            unsafe_allow_html=True,
        )
    elif st.session_state.ollama_ok and not st.session_state.ollama_has_model:
        st.markdown(
            f"<div class='sidebar-card warn'>⚠️ Ollama OK, mas modelo ausente<br>"
            f"<code>ollama pull {OLLAMA_MODEL}</code></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div class='sidebar-card err'>❌ <b>Ollama offline</b><br>"
            "<code>ollama serve</code></div>",
            unsafe_allow_html=True,
        )

    # MCP
    if st.session_state.mcp_ok:
        n = len(st.session_state.mcp_tools)
        st.markdown(
            f"<div class='sidebar-card ok'>✅ <b>MCP Server</b> OK<br>"
            f"{n} ferramentas carregadas</div>",
            unsafe_allow_html=True,
        )
    else:
        err_short = (st.session_state.mcp_error or "")[:80]
        st.markdown(
            f"<div class='sidebar-card err'>❌ <b>MCP Server</b> erro<br>"
            f"<small>{err_short}</small></div>",
            unsafe_allow_html=True,
        )

    # MongoDB
    if st.session_state.mongo_ok:
        st.markdown(
            "<div class='sidebar-card ok'>✅ <b>MongoDB Atlas</b> conectado</div>",
            unsafe_allow_html=True,
        )
    else:
        err_short = (st.session_state.mongo_error or "")[:80]
        st.markdown(
            f"<div class='sidebar-card err'>❌ <b>MongoDB</b> erro<br>"
            f"<small>{err_short}</small></div>",
            unsafe_allow_html=True,
        )

    st.markdown("<hr style='border-color:#30363d; margin: 8px 0'>", unsafe_allow_html=True)

    # Ferramentas MCP
    if st.session_state.mcp_tools:
        with st.expander("🔧 Ferramentas MCP", expanded=False):
            for t in st.session_state.mcp_tools:
                st.markdown(f"**`{t.name}`**")
                st.caption(t.description[:120] + ("…" if len(t.description) > 120 else ""))

    # Ações
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ Limpar", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
    with col2:
        if st.button("🔄 Reconectar", use_container_width=True):
            # Força recheck na próxima execução
            if st.session_state.mcp_client:
                try:
                    st.session_state.mcp_client._stop()
                except Exception:
                    pass
            st.session_state.system_checked = False
            st.session_state.mcp_client = None
            st.session_state.mcp_ok = False
            st.rerun()

    # Info
    st.markdown("<hr style='border-color:#30363d; margin: 8px 0'>", unsafe_allow_html=True)
    st.caption(f"Modelo: `{OLLAMA_MODEL}`")
    st.caption(f"Host: `{OLLAMA_HOST}`")
    st.caption(f"Timeout: `{OLLAMA_TIMEOUT}s`")


# ---------------------------------------------------------------------------
# Área principal
# ---------------------------------------------------------------------------

st.markdown("""
<div class="main-header">
    <h1>🏛️ PNCP Chatbot</h1>
    <p>Pipeline ETL · Bronze → Silver → Gold · Dispensa de Licitação</p>
</div>
""", unsafe_allow_html=True)

# Alertas
if not st.session_state.ollama_ok or not st.session_state.ollama_has_model:
    st.error(
        f"⚠️ Ollama não está pronto. Verifique se o serviço está rodando (`ollama serve`) "
        f"e se o modelo `{OLLAMA_MODEL}` foi baixado (`ollama pull {OLLAMA_MODEL}`)."
    )

if not st.session_state.mcp_ok:
    st.warning(
        f"⚠️ MCP Server não disponível: {st.session_state.mcp_error}. "
        "Verifique se `mcp_server.py` está no mesmo diretório e se as dependências estão instaladas."
    )

# Sugestões (apenas sem histórico)
if not st.session_state.messages:
    st.markdown("#### 💡 Sugestões de perguntas")
    suggestions = [
        "📊 Qual é o estado com mais contratações?",
        "💰 Quais são as faixas de valor mais comuns?",
        "🔧 Qual ramo MEI tem maior valor total?",
        "🏛️ Quais órgãos mais contratam em PE?",
        "📈 Como evoluíram as contratações mês a mês?",
        "📋 Quantos registros existem em cada camada do pipeline?",
        "🔍 Busque contratações de TI publicadas em 2026",
        "📌 Qual a situação mais comum das contratações?",
    ]
    cols = st.columns(2)
    for i, sug in enumerate(suggestions):
        with cols[i % 2]:
            if st.button(sug, key=f"sug_{i}", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": sug})
                st.rerun()
    st.markdown("---")

# Histórico de chat
for msg in st.session_state.messages:
    if msg["role"] == "user":
        with st.chat_message("user", avatar="👤"):
            st.markdown(msg["content"])
    elif msg["role"] == "assistant":
        with st.chat_message("assistant", avatar="🏛️"):
            st.markdown(msg["content"])
            if msg.get("tool_calls_log"):
                n = len(msg["tool_calls_log"])
                with st.expander(f"🔧 {n} consulta(s) ao banco de dados", expanded=False):
                    for tc in msg["tool_calls_log"]:
                        st.markdown(f"**Ferramenta:** `{tc['tool']}`")
                        if tc.get("args"):
                            st.json(tc["args"])
                        st.caption("Resultado (prévia):")
                        st.code(tc["result_preview"], language="json")

# Input do usuário
system_ready = (
    st.session_state.ollama_ok
    and st.session_state.ollama_has_model
    and st.session_state.mcp_ok
    and st.session_state.mcp_client is not None
)

if prompt := st.chat_input(
    "Pergunte sobre as contratações do PNCP…",
    disabled=not system_ready,
):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)

    with st.chat_message("assistant", avatar="🏛️"):
        status_ph = st.empty()
        response_ph = st.empty()

        history = [
            m for m in st.session_state.messages[:-1]
            if m["role"] in ("user", "assistant")
        ]

        response_text, tool_calls_log = agent_respond(
            user_message=prompt,
            history=history,
            mcp_client=st.session_state.mcp_client,
            tools=st.session_state.mcp_tools,
            status_placeholder=status_ph,
        )

        status_ph.empty()
        response_ph.markdown(response_text)

        if tool_calls_log:
            n = len(tool_calls_log)
            with st.expander(f"🔧 {n} consulta(s) ao banco de dados", expanded=False):
                for tc in tool_calls_log:
                    st.markdown(f"**Ferramenta:** `{tc['tool']}`")
                    if tc.get("args"):
                        st.json(tc["args"])
                    st.caption("Resultado (prévia):")
                    st.code(tc["result_preview"], language="json")

    st.session_state.messages.append({
        "role": "assistant",
        "content": response_text,
        "tool_calls_log": tool_calls_log,
    })

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#8b949e; font-size:0.75rem; font-family:IBM Plex Mono,monospace'>"
    "PNCP Pipeline · data-orchestration · Powered by Ollama + MCP"
    "</div>",
    unsafe_allow_html=True,
)