"""
Chatbot PNCP — Interface Streamlit
Conecta-se ao mcp_server.py via subprocesso (stdio) e usa Ollama como LLM.

Requisitos:
    pip install streamlit mcp pymongo certifi ollama
    # E ter o Ollama instalado: https://ollama.com
    # Baixar o modelo: ollama pull llama3.2 (ou outro listado no README)
"""

import asyncio
import json
import subprocess
import sys
import os
import re
from datetime import datetime

import streamlit as st
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
import ollama

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://larissa:engbigdata%40@cluster-bi-data.fzpirih.mongodb.net/?appName=cluster-bi-data",
)

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")

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

MCP_SERVER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mcp_server.py")

# ---------------------------------------------------------------------------
# Funções auxiliares MCP
# ---------------------------------------------------------------------------

def run_async(coro):
    """Executa uma coroutine de forma síncrona (compatível com Streamlit)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


async def _list_tools_async():
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[MCP_SERVER_PATH],
        env={**os.environ, "MONGO_URI": MONGO_URI},
    )
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools_response = await session.list_tools()
            return tools_response.tools


async def _call_tool_async(tool_name: str, arguments: dict) -> str:
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[MCP_SERVER_PATH],
        env={**os.environ, "MONGO_URI": MONGO_URI},
    )
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(tool_name, arguments)
            if result.content:
                return result.content[0].text
            return "{}"


def call_mcp_tool(tool_name: str, arguments: dict) -> str:
    return run_async(_call_tool_async(tool_name, arguments))


def get_mcp_tools() -> list:
    return run_async(_list_tools_async())


# ---------------------------------------------------------------------------
# Lógica do agente com Ollama
# ---------------------------------------------------------------------------

def tools_to_ollama_format(mcp_tools) -> list[dict]:
    """Converte ferramentas MCP para o formato esperado pelo Ollama."""
    ollama_tools = []
    for t in mcp_tools:
        ollama_tools.append({
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.inputSchema,
            },
        })
    return ollama_tools


def agent_respond(user_message: str, history: list, mcp_tools) -> tuple[str, list]:
    """
    Executa o loop agentico: chama Ollama, detecta tool_calls,
    executa as ferramentas via MCP e retorna a resposta final.
    """
    ollama_tools = tools_to_ollama_format(mcp_tools)

    # Monta histórico no formato Ollama
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    tool_calls_log = []
    max_iterations = 5  # Evita loop infinito

    for _ in range(max_iterations):
        try:
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=messages,
                tools=ollama_tools,
            )
        except Exception as e:
            return f"❌ Erro ao conectar ao Ollama: {e}\n\nVerifique se o Ollama está rodando (`ollama serve`) e se o modelo '{OLLAMA_MODEL}' foi baixado (`ollama pull {OLLAMA_MODEL}`).", tool_calls_log

        msg = response.message

        # Sem tool calls → resposta final
        if not msg.tool_calls:
            final_text = msg.content or ""
            return final_text, tool_calls_log

        # Processar tool calls
        messages.append({"role": "assistant", "content": msg.content or "", "tool_calls": [
            {
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                }
            }
            for tc in msg.tool_calls
        ]})

        for tc in msg.tool_calls:
            tool_name = tc.function.name
            arguments = tc.function.arguments if isinstance(tc.function.arguments, dict) else {}

            # Executar ferramenta via MCP
            with st.spinner(f"🔧 Consultando `{tool_name}`..."):
                tool_result = call_mcp_tool(tool_name, arguments)

            tool_calls_log.append({
                "tool": tool_name,
                "args": arguments,
                "result_preview": tool_result[:300] + "..." if len(tool_result) > 300 else tool_result,
            })

            messages.append({
                "role": "tool",
                "content": tool_result,
            })

    return "⚠️ Número máximo de iterações atingido.", tool_calls_log


# ---------------------------------------------------------------------------
# Interface Streamlit
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="PNCP Chatbot",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS customizado
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

    .main-header h1 {
        font-size: 2rem;
        font-weight: 800;
        color: var(--text);
        margin: 0;
        letter-spacing: -0.5px;
    }

    .main-header p {
        color: var(--text-muted);
        margin: 4px 0 0 0;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.8rem;
    }

    .chat-user {
        background: linear-gradient(135deg, #1c2333, #21262d);
        border: 1px solid var(--border);
        border-left: 3px solid var(--accent);
        border-radius: 0 10px 10px 10px;
        padding: 14px 18px;
        margin: 8px 0;
        max-width: 85%;
    }

    .chat-assistant {
        background: linear-gradient(135deg, #161b22, #1c2333);
        border: 1px solid var(--border);
        border-left: 3px solid var(--accent2);
        border-radius: 0 10px 10px 10px;
        padding: 14px 18px;
        margin: 8px 0;
        max-width: 95%;
    }

    .tool-badge {
        display: inline-block;
        background: rgba(124, 58, 237, 0.15);
        border: 1px solid rgba(124, 58, 237, 0.4);
        color: #a78bfa;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.72rem;
        padding: 2px 8px;
        border-radius: 4px;
        margin: 2px;
    }

    .stat-card {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 16px;
        text-align: center;
    }

    .stat-number {
        font-size: 1.8rem;
        font-weight: 800;
        color: var(--accent);
        font-family: 'IBM Plex Mono', monospace;
    }

    .stat-label {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-top: 4px;
    }

    .suggestion-btn {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        color: var(--text) !important;
        border-radius: 8px !important;
        font-size: 0.82rem !important;
        padding: 8px 14px !important;
        text-align: left !important;
    }

    .sidebar-section {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 14px;
        margin-bottom: 12px;
    }

    div[data-testid="stChatMessage"] {
        background: transparent !important;
    }

    .stChatInputContainer > div {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
    }

    .stButton > button {
        background: var(--surface);
        border: 1px solid var(--border);
        color: var(--text);
        border-radius: 8px;
        font-family: 'Syne', sans-serif;
        transition: all 0.2s;
    }

    .stButton > button:hover {
        border-color: var(--accent);
        color: var(--accent);
    }

    code {
        font-family: 'IBM Plex Mono', monospace;
        background: rgba(0,229,255,0.08);
        color: var(--accent);
        padding: 1px 5px;
        border-radius: 3px;
        font-size: 0.85em;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Inicialização de estado
# ---------------------------------------------------------------------------

if "messages" not in st.session_state:
    st.session_state.messages = []
if "tools_loaded" not in st.session_state:
    st.session_state.tools_loaded = False
if "mcp_tools" not in st.session_state:
    st.session_state.mcp_tools = []
if "ollama_ok" not in st.session_state:
    st.session_state.ollama_ok = False


# ---------------------------------------------------------------------------
# Verificações de status
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def check_ollama():
    try:
        models = ollama.list()
        model_names = [m.model for m in models.models]
        has_model = any(OLLAMA_MODEL in name for name in model_names)
        return True, model_names, has_model
    except Exception as e:
        return False, [], False


@st.cache_resource(show_spinner=False)
def load_mcp_tools():
    try:
        tools = get_mcp_tools()
        return tools, None
    except Exception as e:
        return [], str(e)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### 🏛️ PNCP Chatbot")
    st.markdown("<hr style='border-color: #30363d'>", unsafe_allow_html=True)

    # Status Ollama
    ollama_ok, available_models, has_model = check_ollama()
    if ollama_ok and has_model:
        st.markdown(f"<div class='sidebar-section'>✅ <b>Ollama</b> conectado<br><code>{OLLAMA_MODEL}</code></div>", unsafe_allow_html=True)
        st.session_state.ollama_ok = True
    elif ollama_ok and not has_model:
        st.markdown(f"<div class='sidebar-section'>⚠️ Modelo <code>{OLLAMA_MODEL}</code> não encontrado.<br><br>Execute:<br><code>ollama pull {OLLAMA_MODEL}</code></div>", unsafe_allow_html=True)
    else:
        st.markdown("<div class='sidebar-section'>❌ <b>Ollama offline</b><br>Execute: <code>ollama serve</code></div>", unsafe_allow_html=True)

    # Status MCP
    mcp_tools, mcp_error = load_mcp_tools()
    if mcp_tools:
        st.markdown(f"<div class='sidebar-section'>✅ <b>MCP Server</b> OK<br>{len(mcp_tools)} ferramentas carregadas</div>", unsafe_allow_html=True)
        st.session_state.mcp_tools = mcp_tools
        st.session_state.tools_loaded = True
    else:
        st.markdown(f"<div class='sidebar-section'>❌ <b>MCP Server</b> erro<br><small>{mcp_error}</small></div>", unsafe_allow_html=True)

    st.markdown("---")

    # Ferramentas disponíveis
    if st.session_state.mcp_tools:
        with st.expander("🔧 Ferramentas MCP", expanded=False):
            for t in st.session_state.mcp_tools:
                st.markdown(f"**`{t.name}`**")
                st.caption(t.description[:120] + "...")

    # Configuração do modelo
    with st.expander("⚙️ Configuração", expanded=False):
        if ollama_ok and available_models:
            selected_model = st.selectbox(
                "Modelo Ollama",
                options=[m for m in available_models],
                index=0,
            )
            if selected_model != OLLAMA_MODEL:
                os.environ["OLLAMA_MODEL"] = selected_model
        st.caption(f"Modelo atual: `{OLLAMA_MODEL}`")
        st.caption(f"MCP Server: `mcp_server.py`")

    # Limpar conversa
    if st.button("🗑️ Limpar conversa", use_container_width=True):
        st.session_state.messages = []
        st.rerun()


# ---------------------------------------------------------------------------
# Área principal
# ---------------------------------------------------------------------------

# Header
st.markdown("""
<div class="main-header">
    <h1>🏛️ PNCP Chatbot</h1>
    <p>Pipeline ETL · Bronze → Silver → Gold · Dispensa de Licitação</p>
</div>
""", unsafe_allow_html=True)


# Alertas de configuração
if not st.session_state.ollama_ok:
    st.error("⚠️ Ollama não está acessível. Veja as instruções no README para iniciar.")

if not st.session_state.tools_loaded:
    st.warning("⚠️ MCP Server não está respondendo. Verifique se `mcp_server.py` está no mesmo diretório.")


# Sugestões de perguntas (só quando sem histórico)
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


# Exibir histórico de mensagens
for msg in st.session_state.messages:
    if msg["role"] == "user":
        with st.chat_message("user", avatar="👤"):
            st.markdown(msg["content"])
    elif msg["role"] == "assistant":
        with st.chat_message("assistant", avatar="🏛️"):
            st.markdown(msg["content"])
            # Mostrar ferramentas usadas
            if msg.get("tool_calls_log"):
                with st.expander(f"🔧 {len(msg['tool_calls_log'])} consulta(s) ao banco", expanded=False):
                    for tc in msg["tool_calls_log"]:
                        st.markdown(f"**Ferramenta:** `{tc['tool']}`")
                        if tc["args"]:
                            st.json(tc["args"])
                        st.caption("Resultado (prévia):")
                        st.code(tc["result_preview"], language="json")


# Input do usuário
if prompt := st.chat_input("Pergunte sobre as contratações do PNCP..."):
    # Adiciona mensagem do usuário
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)

    # Verifica se pode responder
    if not st.session_state.tools_loaded or not st.session_state.ollama_ok:
        st.error("Sistema não está pronto. Verifique Ollama e MCP Server na barra lateral.")
    else:
        with st.chat_message("assistant", avatar="🏛️"):
            with st.spinner("🤔 Analisando sua pergunta..."):
                # Histórico sem a última mensagem do usuário (já incluída no agent_respond)
                history = [
                    m for m in st.session_state.messages[:-1]
                    if m["role"] in ("user", "assistant")
                ]
                response_text, tool_calls_log = agent_respond(
                    prompt,
                    history,
                    st.session_state.mcp_tools,
                )

            st.markdown(response_text)

            if tool_calls_log:
                with st.expander(f"🔧 {len(tool_calls_log)} consulta(s) ao banco", expanded=False):
                    for tc in tool_calls_log:
                        st.markdown(f"**Ferramenta:** `{tc['tool']}`")
                        if tc["args"]:
                            st.json(tc["args"])
                        st.caption("Resultado (prévia):")
                        st.code(tc["result_preview"], language="json")

        # Salva no histórico
        st.session_state.messages.append({
            "role": "assistant",
            "content": response_text,
            "tool_calls_log": tool_calls_log,
        })


# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color: #8b949e; font-size: 0.75rem; font-family: IBM Plex Mono, monospace'>"
    "PNCP Pipeline · data-orchestration · Powered by Ollama + MCP"
    "</div>",
    unsafe_allow_html=True,
)
