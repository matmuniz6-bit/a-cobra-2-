import os
import time
import asyncio
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, TypeHandler, filters

from .client import post, get
from .metrics import incr_counter

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8080")

# in-memory onboarding state (mínimo viável)
STATE = {}

UFS = ["SP", "RJ", "MG", "RS", "SC", "PR", "BA", "PE", "CE", "DF", "GO"]
MODALIDADES = ["pregao", "concorrencia", "dispensa", "outros"]
CATEGORIAS = ["limpeza", "ti", "saude", "vigilancia", "manutencao", "obras", "outros"]
KEYWORDS = ["limpeza", "ti", "saude", "vigilancia", "manutencao", "obras", "servicos", "materiais", "nenhuma"]

MUNICIPIOS = {
    "SP": ["Sao Paulo", "Campinas", "Santos", "Ribeirao Preto", "Sorocaba"],
    "RJ": ["Rio de Janeiro", "Niteroi", "Duque de Caxias", "Nova Iguacu", "Petropolis"],
    "MG": ["Belo Horizonte", "Uberlandia", "Contagem", "Juiz de Fora", "Betim"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas", "Canoas", "Santa Maria"],
    "SC": ["Florianopolis", "Joinville", "Blumenau", "Chapeco", "Itajai"],
    "PR": ["Curitiba", "Londrina", "Maringa", "Ponta Grossa", "Cascavel"],
    "BA": ["Salvador", "Feira de Santana", "Vitoria da Conquista", "Camacari", "Ilheus"],
    "PE": ["Recife", "Jaboatao dos Guararapes", "Olinda", "Caruaru", "Petrolina"],
    "CE": ["Fortaleza", "Caucaia", "Juazeiro do Norte", "Maracanau", "Sobral"],
    "DF": ["Brasilia"],
    "GO": ["Goiania", "Aparecida de Goiania", "Anapolis", "Rio Verde", "Luziania"],
}


def _kb(rows):
    return InlineKeyboardMarkup(rows)


def _user_payload(update: Update):
    u = update.effective_user
    if not u:
        return None
    return {
        "telegram_user_id": u.id,
        "username": u.username,
        "first_name": u.first_name,
        "last_name": u.last_name,
        "language_code": u.language_code,
    }

def _set_current_tender(user_id: int, tender_id: int):
    st = STATE.get(user_id, {})
    st["current_tender_id"] = tender_id
    st["awaiting_question"] = False
    STATE[user_id] = st

def _get_current_tender(user_id: int):
    st = STATE.get(user_id, {})
    return st.get("current_tender_id")

def _set_awaiting_question(user_id: int, value: bool):
    st = STATE.get(user_id, {})
    st["awaiting_question"] = value
    STATE[user_id] = st

def _is_awaiting_question(user_id: int) -> bool:
    st = STATE.get(user_id, {})
    return bool(st.get("awaiting_question"))

def _set_edit_subscription(user_id: int, sub_id: int | None):
    st = STATE.get(user_id, {})
    if sub_id is None:
        st.pop("edit_subscription_id", None)
        st.pop("edit_mode", None)
    else:
        st["edit_subscription_id"] = int(sub_id)
        st["edit_mode"] = True
    STATE[user_id] = st

def _lic_menu_kb():
    return _kb([
        [InlineKeyboardButton("Resumo", callback_data="lic_summary")],
        [InlineKeyboardButton("Checklist", callback_data="lic_checklist")],
        [InlineKeyboardButton("Prazos / Sessao", callback_data="lic_prazos")],
        [InlineKeyboardButton("Documentos", callback_data="lic_docs")],
        [InlineKeyboardButton("Perguntar", callback_data="lic_ask")],
        [InlineKeyboardButton("Voltar ao Hub", callback_data="hub")],
    ])

def _cb_city(name: str) -> str:
    return name.replace(" ", "_")

def _cb_city_decode(code: str) -> str:
    return code.replace("_", " ")

def _capital_for_uf(uf: str) -> str | None:
    lst = MUNICIPIOS.get(uf or "")
    return lst[0] if lst else None

def _municipios_for_uf(uf: str) -> list[str]:
    return MUNICIPIOS.get(uf or "", [])


async def _api_post(url: str, payload: dict):
    return await asyncio.to_thread(post, url, payload)


async def _api_get(url: str):
    return await asyncio.to_thread(get, url)


async def _metrics_capture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await incr_counter("bot.updates_total")
    try:
        if update.callback_query:
            await incr_counter("bot.callbacks_total")
        if update.message:
            text = (update.message.text or "").strip()
            if text.startswith("/"):
                await incr_counter("bot.commands_total")
            else:
                await incr_counter("bot.messages_total")
    except Exception:
        await incr_counter("bot.errors_total")


async def _error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    await incr_counter("bot.errors_total")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not TOKEN:
        if update.message:
            await update.message.reply_text("Bot não configurado.")
        return

    u = _user_payload(update)
    if u:
        try:
            await _api_post(f"{API_BASE_URL}/v1/users/upsert", u)
        except Exception:
            pass

    # deep link: /start qa_<tender_id>
    if context.args:
        arg = context.args[0]
        if arg.startswith("qa_"):
            tid = arg.split("qa_", 1)[1]
            if tid.isdigit():
                await _open_tender(update, int(tid))
                return
        if arg.startswith("follow_"):
            tid = arg.split("follow_", 1)[1]
            if tid.isdigit():
                try:
                    await _api_post(
                        f"{API_BASE_URL}/v1/users/follow",
                        {"telegram_user_id": update.effective_user.id, "tender_id": int(tid)},
                    )
                    await update.message.reply_text("✅ Você começou a seguir esta licitação.")
                except Exception:
                    await update.message.reply_text("Não consegui seguir agora. Tente novamente.")
                return
        if arg.startswith("setup_"):
            uf = arg.split("setup_", 1)[1].upper()
            if uf in UFS and update.message:
                await _onboard_start_message(update, preset_uf=uf)
                return

    text = "Bem-vindo. Vou te ajudar a acompanhar licitações com filtros e alertas."
    buttons = [
        [InlineKeyboardButton("Começar", callback_data="onboard_start")],
        [InlineKeyboardButton("Como funciona", callback_data="how_it_works")],
        [InlineKeyboardButton("Planos / Acesso", callback_data="plans")],
        [InlineKeyboardButton("Falar com suporte", callback_data="support")],
    ]
    if update.message:
        await update.message.reply_text(text, reply_markup=_kb(buttons))


async def _onboard_start_message(update: Update, preset_uf: str | None = None):
    u = update.effective_user
    if not u or not update.message:
        return
    st = STATE.get(u.id, {})
    st["filters"] = {}
    STATE[u.id] = st
    if preset_uf:
        st["filters"]["uf"] = [preset_uf]
        STATE[u.id] = st
        municipios = _municipios_for_uf(preset_uf)
        rows = []
        for m in municipios:
            rows.append([InlineKeyboardButton(m, callback_data=f"onboard_mun:{_cb_city(m)}")])
        rows.append([InlineKeyboardButton("Todos os municipios", callback_data="onboard_mun:ALL")])
        if _capital_for_uf(preset_uf):
            rows.append([InlineKeyboardButton("Apenas capital", callback_data="onboard_mun:CAP")])
        rows.append([InlineKeyboardButton("Pular municipio", callback_data="onboard_mun:SKIP")])
        await update.message.reply_text(f"UF pré-selecionada: {preset_uf}\nEscolha o municipio:", reply_markup=_kb(rows))
        return
    # fallback: mesma UI do callback
    rows = []
    row = []
    for uf in UFS:
        row.append(InlineKeyboardButton(uf, callback_data=f"onboard_uf:{uf}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Todos", callback_data="onboard_uf:ALL")])
    await update.message.reply_text("Escolha a UF:", reply_markup=_kb(rows))

async def start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    text = "Bem-vindo. Vou te ajudar a acompanhar licitações com filtros e alertas."
    buttons = [
        [InlineKeyboardButton("Começar", callback_data="onboard_start")],
        [InlineKeyboardButton("Como funciona", callback_data="how_it_works")],
        [InlineKeyboardButton("Planos / Acesso", callback_data="plans")],
        [InlineKeyboardButton("Falar com suporte", callback_data="support")],
    ]
    await q.edit_message_text(text, reply_markup=_kb(buttons))

async def _open_tender(update: Update, tender_id: int):
    u = update.effective_user
    if not u or not update.message:
        return
    _set_current_tender(u.id, tender_id)
    await update.message.reply_text(
        f"Sala da licitacao #{tender_id}",
        reply_markup=_lic_menu_kb(),
    )


async def hub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        _set_edit_subscription(q.from_user.id, None)
        buttons = [
            [InlineKeyboardButton("Minhas assinaturas", callback_data="subs_list")],
            [InlineKeyboardButton("Criar assinatura", callback_data="onboard_start")],
            [InlineKeyboardButton("Editar filtros", callback_data="subs_edit")],
            [InlineKeyboardButton("Pausar alertas", callback_data="subs_pause")],
            [InlineKeyboardButton("Resumo diário", callback_data="subs_daily")],
            [InlineKeyboardButton("Ajuda", callback_data="help")],
            [InlineKeyboardButton("Conta / Plano", callback_data="plans")],
            [InlineKeyboardButton("Falar com suporte", callback_data="support")],
        ]
        await q.edit_message_text("Hub — escolha uma opção:", reply_markup=_kb(buttons))


async def how_it_works(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        text = (
            "Como funciona (rápido):\n"
            "1) Você escolhe filtros (UF/municipio/modalidade/area).\n"
            "2) Recebe alertas.\n"
            "3) Ao abrir um alerta, entra na sala privada da licitacao para resumo, checklist e perguntas."
        )
        await q.edit_message_text(text, reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="start")]]))


async def plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        text = "Plano atual: Free. Limites serão exibidos aqui."
        await q.edit_message_text(text, reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="start")]]))


async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        await q.edit_message_text(
            "Suporte: descreva sua dúvida e retornaremos por aqui.",
            reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="start")]]),
        )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        await q.edit_message_text(
            "Ajuda rápida: use o Hub para gerenciar filtros e alertas.",
            reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]),
        )

async def resumo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    args = (update.message.text or "").strip().split()
    if len(args) < 2 or not args[1].isdigit():
        await update.message.reply_text("Uso: /resumo <tender_id>")
        return
    tid = int(args[1])
    try:
        data = await _api_post(f"{API_BASE_URL}/v1/insights/extract", {"tender_id": tid})
    except Exception:
        data = {}
    fields = (data or {}).get("fields") or {}
    if not fields:
        await update.message.reply_text("Não encontrei campos estruturados para este tender.")
        return
    lines = []
    if fields.get("objeto"):
        lines.append(f"Objeto: {fields['objeto']}")
    if fields.get("valor"):
        lines.append(f"Valor: {fields['valor']}")
    if fields.get("sessao"):
        lines.append(f"Sessão: {fields['sessao']}")
    if fields.get("prazo_proposta"):
        lines.append(f"Prazo proposta: {fields['prazo_proposta']}")
    if fields.get("modalidade"):
        lines.append(f"Modalidade: {fields['modalidade']}")
    if fields.get("orgao"):
        lines.append(f"Órgão: {fields['orgao']}")
    await update.message.reply_text("\n".join(lines))

async def lic_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    tid = _get_current_tender(q.from_user.id)
    if not tid:
        await q.edit_message_text("Nenhuma licitacao ativa.", reply_markup=_lic_menu_kb())
        return
    try:
        data = await _api_post(f"{API_BASE_URL}/v1/insights/extract", {"tender_id": tid})
        fields = (data or {}).get("fields") or {}
    except Exception:
        fields = {}
    if not fields:
        try:
            data = await _api_post(f"{API_BASE_URL}/v1/insights/summary", {"tender_id": tid})
            bullets = (data or {}).get("bullets") or []
        except Exception:
            bullets = []
        text = "\n".join([f"- {b}" for b in bullets]) if bullets else "Sem resumo disponivel."
    else:
        lines = []
        if fields.get("objeto"):
            lines.append(f"Objeto: {fields['objeto']}")
        if fields.get("valor"):
            lines.append(f"Valor: {fields['valor']}")
        if fields.get("sessao"):
            lines.append(f"Sessao: {fields['sessao']}")
        if fields.get("prazo_proposta"):
            lines.append(f"Prazo proposta: {fields['prazo_proposta']}")
        if fields.get("modalidade"):
            lines.append(f"Modalidade: {fields['modalidade']}")
        if fields.get("orgao"):
            lines.append(f"Orgao: {fields['orgao']}")
        text = "\n".join(lines) if lines else "Sem resumo disponivel."
    await q.edit_message_text(text, reply_markup=_lic_menu_kb())

async def lic_checklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    tid = _get_current_tender(q.from_user.id)
    if not tid:
        await q.edit_message_text("Nenhuma licitacao ativa.", reply_markup=_lic_menu_kb())
        return
    try:
        data = await _api_post(f"{API_BASE_URL}/v1/insights/checklist", {"tender_id": tid})
    except Exception:
        data = {}
    items = (data or {}).get("items") or []
    if not items:
        text = "Checklist indisponivel."
    else:
        text = "Checklist:\n" + "\n".join([f"- {i.get('title')} ({i.get('priority')})" for i in items])
    await q.edit_message_text(text, reply_markup=_lic_menu_kb())

async def lic_prazos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    tid = _get_current_tender(q.from_user.id)
    if not tid:
        await q.edit_message_text("Nenhuma licitacao ativa.", reply_markup=_lic_menu_kb())
        return
    try:
        data = await _api_post(f"{API_BASE_URL}/v1/insights/extract", {"tender_id": tid})
        fields = (data or {}).get("fields") or {}
    except Exception:
        fields = {}
    lines = []
    if fields.get("sessao"):
        lines.append(f"Sessao: {fields['sessao']}")
    if fields.get("prazo_proposta"):
        lines.append(f"Prazo proposta: {fields['prazo_proposta']}")
    text = "\n".join(lines) if lines else "Nao localizei prazos neste edital."
    await q.edit_message_text(text, reply_markup=_lic_menu_kb())

async def lic_docs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    tid = _get_current_tender(q.from_user.id)
    if not tid:
        await q.edit_message_text("Nenhuma licitacao ativa.", reply_markup=_lic_menu_kb())
        return
    try:
        data = await _api_get(f"{API_BASE_URL}/v1/documents/list?tender_id={tid}&limit=5")
    except Exception:
        data = {}
    items = (data or {}).get("items") or []
    if not items:
        text = "Sem documentos encontrados."
    else:
        lines = ["Documentos:"]
        for it in items:
            url = it.get("url") or ""
            lines.append(url)
        text = "\n".join(lines)
    await q.edit_message_text(text, reply_markup=_lic_menu_kb())

async def lic_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    tid = _get_current_tender(q.from_user.id)
    if not tid:
        await q.edit_message_text("Nenhuma licitacao ativa.", reply_markup=_lic_menu_kb())
        return
    _set_awaiting_question(q.from_user.id, True)
    await q.edit_message_text("Envie sua pergunta:", reply_markup=_lic_menu_kb())

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    u = update.effective_user
    if not u:
        return
    if not _is_awaiting_question(u.id):
        return
    tid = _get_current_tender(u.id)
    if not tid:
        await update.message.reply_text("Nenhuma licitacao ativa.")
        return
    _set_awaiting_question(u.id, False)
    question = update.message.text or ""
    try:
        data = await _api_post(f"{API_BASE_URL}/v1/insights/qa", {"tender_id": tid, "question": question})
    except Exception:
        data = {}
    answer = (data or {}).get("answer") or "Nao consegui responder agora."
    ev = (data or {}).get("evidence") or []
    if ev:
        snippet = (ev[0].get("text") or "")[:200].replace("\n", " ")
        answer = f"{answer}\n\nTrecho: {snippet}"
    await update.message.reply_text(answer)


async def subs_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    u = q.from_user
    if not u:
        await q.edit_message_text("Usuário não encontrado.")
        return
    try:
        data = await _api_get(f"{API_BASE_URL}/v1/subscriptions/list?telegram_user_id={u.id}")
    except Exception:
        data = {"items": []}
    items = data.get("items", []) if isinstance(data, dict) else []
    if not items:
        text = "Você ainda não tem assinaturas."
    else:
        text = "Assinaturas:\n" + "\n".join([f"- #{i['id']} ({i.get('frequency','realtime')})" for i in items])
    await q.edit_message_text(text, reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))


async def onboard_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    st = STATE.get(q.from_user.id, {})
    if not st.get("edit_mode"):
        _set_edit_subscription(q.from_user.id, None)
        st = STATE.get(q.from_user.id, {})
    st["filters"] = {}
    STATE[q.from_user.id] = st
    rows = []
    row = []
    for uf in UFS:
        row.append(InlineKeyboardButton(uf, callback_data=f"onboard_uf:{uf}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Todos", callback_data="onboard_uf:ALL")])
    await q.edit_message_text("Escolha a UF:", reply_markup=_kb(rows))


async def onboard_uf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    uf = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    st["filters"]["uf"] = [uf] if uf != "ALL" else ["ALL"]
    STATE[q.from_user.id] = st

    uf_val = uf if uf != "ALL" else "SP"
    municipios = _municipios_for_uf(uf_val)
    rows = []
    for m in municipios:
        rows.append([InlineKeyboardButton(m, callback_data=f"onboard_mun:{_cb_city(m)}")])
    rows.append([InlineKeyboardButton("Todos os municipios", callback_data="onboard_mun:ALL")])
    if _capital_for_uf(uf_val):
        rows.append([InlineKeyboardButton("Apenas capital", callback_data="onboard_mun:CAP")])
    rows.append([InlineKeyboardButton("Pular municipio", callback_data="onboard_mun:SKIP")])
    await q.edit_message_text("Escolha o municipio:", reply_markup=_kb(rows))

async def onboard_mun(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    code = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    uf = (st.get("filters", {}).get("uf") or ["SP"])[0]
    if code == "ALL":
        st["filters"]["municipio"] = ["ALL"]
    elif code == "CAP":
        cap = _capital_for_uf(uf) or ""
        if cap:
            st["filters"]["municipio"] = [cap]
    elif code == "SKIP":
        st["filters"].pop("municipio", None)
    else:
        st["filters"]["municipio"] = [_cb_city_decode(code)]
    STATE[q.from_user.id] = st

    rows = []
    row = []
    for mod in MODALIDADES:
        label = mod.capitalize()
        row.append(InlineKeyboardButton(label, callback_data=f"onboard_mod:{mod}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    await q.edit_message_text("Escolha a modalidade:", reply_markup=_kb(rows))


async def onboard_mod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    mod = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    st["filters"]["modalidade"] = [mod]
    STATE[q.from_user.id] = st

    rows = []
    row = []
    for cat in CATEGORIAS:
        label = cat.upper() if len(cat) <= 3 else cat.capitalize()
        row.append(InlineKeyboardButton(label, callback_data=f"onboard_cat:{cat}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    await q.edit_message_text("Escolha a área/categoria:", reply_markup=_kb(rows))


async def onboard_cat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    cat = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    st["filters"]["categoria"] = [cat]
    STATE[q.from_user.id] = st

    rows = [
        [InlineKeyboardButton("Escolher palavra-chave", callback_data="onboard_kw:open")],
        [InlineKeyboardButton("Pular palavra-chave", callback_data="onboard_kw:skip")],
    ]
    await q.edit_message_text("Palavras-chave:", reply_markup=_kb(rows))

async def onboard_kw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    action = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    if action == "skip":
        st["filters"].pop("keywords", None)
        STATE[q.from_user.id] = st
        rows = [
            [InlineKeyboardButton("Somente novas", callback_data="onboard_rep:new")],
            [InlineKeyboardButton("Incluir republicacoes", callback_data="onboard_rep:all")],
        ]
        await q.edit_message_text("Alertas:", reply_markup=_kb(rows))
        return

    rows = []
    row = []
    for kw in KEYWORDS:
        label = kw.upper() if len(kw) <= 3 else kw.capitalize()
        row.append(InlineKeyboardButton(label, callback_data=f"onboard_kw_pick:{kw}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    await q.edit_message_text("Escolha uma palavra-chave:", reply_markup=_kb(rows))

async def onboard_kw_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    kw = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    if kw == "nenhuma":
        st["filters"].pop("keywords", None)
    else:
        st["filters"]["keywords"] = [kw]
    STATE[q.from_user.id] = st
    rows = [
        [InlineKeyboardButton("Somente novas", callback_data="onboard_rep:new")],
        [InlineKeyboardButton("Incluir republicacoes", callback_data="onboard_rep:all")],
    ]
    await q.edit_message_text("Alertas:", reply_markup=_kb(rows))

async def onboard_rep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    mode = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    st["filters"]["republicacoes"] = "all" if mode == "all" else "new_only"
    STATE[q.from_user.id] = st
    rows = [
        [InlineKeyboardButton("Tempo real", callback_data="onboard_freq:realtime")],
        [InlineKeyboardButton("Resumo diario", callback_data="onboard_freq:daily")],
    ]
    await q.edit_message_text("Frequencia de alertas:", reply_markup=_kb(rows))


async def onboard_freq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    freq = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    st["frequency"] = freq
    STATE[q.from_user.id] = st

    rows = [
        [InlineKeyboardButton("Canal + PV", callback_data="onboard_delivery:both")],
        [InlineKeyboardButton("Só canal", callback_data="onboard_delivery:channel")],
        [InlineKeyboardButton("Só PV", callback_data="onboard_delivery:pv")],
    ]
    await q.edit_message_text("Onde deseja receber alertas?", reply_markup=_kb(rows))


async def onboard_delivery(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    mode = q.data.split(":", 1)[1]
    st = STATE.get(q.from_user.id, {"filters": {}})
    if mode == "channel":
        st["delivery"] = {"pv": False, "channel": True}
    elif mode == "pv":
        st["delivery"] = {"pv": True, "channel": False}
    else:
        st["delivery"] = {"pv": True, "channel": True}
    STATE[q.from_user.id] = st
    rows = [
        [InlineKeyboardButton("Salvar filtros", callback_data="onboard_save")],
        [InlineKeyboardButton("Voltar ao Hub", callback_data="hub")],
    ]
    await q.edit_message_text("Salvar filtros?", reply_markup=_kb(rows))


async def onboard_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    st = STATE.get(q.from_user.id, {"filters": {}})
    payload = {
        "filters": st.get("filters", {}),
        "delivery": st.get("delivery", {"pv": True, "channel": True}),
        "frequency": st.get("frequency", "realtime"),
    }
    try:
        edit_id = st.get("edit_subscription_id")
        if edit_id:
            payload["id"] = int(edit_id)
            await _api_post(f"{API_BASE_URL}/v1/subscriptions/update", payload)
            text = "Filtros atualizados."
        else:
            payload["telegram_user_id"] = q.from_user.id
            await _api_post(f"{API_BASE_URL}/v1/subscriptions/create", payload)
            text = "Filtros salvos. Você começará a receber alertas."
        _set_edit_subscription(q.from_user.id, None)
    except Exception:
        text = "Não consegui salvar agora. Tente novamente."
    await q.edit_message_text(text, reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))


async def subs_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        u = q.from_user
        if not u:
            await q.edit_message_text("Usuário não encontrado.", reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))
            return
        try:
            data = await _api_get(f"{API_BASE_URL}/v1/subscriptions/list?telegram_user_id={u.id}")
        except Exception:
            data = {"items": []}
        items = data.get("items", []) if isinstance(data, dict) else []
        if not items:
            await q.edit_message_text("Você ainda não tem assinaturas.", reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))
            return
        if len(items) == 1:
            _set_edit_subscription(u.id, items[0]["id"])
            await onboard_start(update, context)
            return
        rows = []
        for it in items:
            sub_id = it.get("id")
            freq = it.get("frequency", "realtime")
            rows.append([InlineKeyboardButton(f"Assinatura #{sub_id} ({freq})", callback_data=f"subs_edit_pick:{sub_id}")])
        rows.append([InlineKeyboardButton("Voltar", callback_data="hub")])
        await q.edit_message_text("Qual assinatura você quer editar?", reply_markup=_kb(rows))

async def subs_edit_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    sub_id = q.data.split(":", 1)[1]
    if not sub_id.isdigit():
        await q.edit_message_text("Assinatura inválida.", reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))
        return
    _set_edit_subscription(q.from_user.id, int(sub_id))
    await onboard_start(update, context)


async def subs_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        try:
            await _api_post(f"{API_BASE_URL}/v1/subscriptions/pause_all", {"telegram_user_id": q.from_user.id, "is_active": False})
            text = "Alertas pausados."
        except Exception:
            text = "Não consegui pausar agora. Tente novamente."
        await q.edit_message_text(text, reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))


async def subs_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()
        try:
            await _api_post(f"{API_BASE_URL}/v1/subscriptions/set_frequency", {"telegram_user_id": q.from_user.id, "frequency": "daily"})
            text = "Resumo diário ativado."
        except Exception:
            text = "Não consegui ativar agora. Tente novamente."
        await q.edit_message_text(text, reply_markup=_kb([[InlineKeyboardButton("Voltar", callback_data="hub")]]))


def main():
    if not TOKEN:
        print("missing_TELEGRAM_BOT_TOKEN")
        while True:
            time.sleep(60)

    app = Application.builder().token(TOKEN).build()
    app.add_handler(TypeHandler(Update, _metrics_capture), group=0)
    app.add_error_handler(_error_handler)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("resumo", resumo_cmd))

    app.add_handler(CallbackQueryHandler(start_cb, pattern="^start$"))
    app.add_handler(CallbackQueryHandler(hub, pattern="^hub$"))
    app.add_handler(CallbackQueryHandler(how_it_works, pattern="^how_it_works$"))
    app.add_handler(CallbackQueryHandler(plans, pattern="^plans$"))
    app.add_handler(CallbackQueryHandler(support, pattern="^support$"))
    app.add_handler(CallbackQueryHandler(help_cmd, pattern="^help$"))
    app.add_handler(CallbackQueryHandler(subs_list, pattern="^subs_list$"))
    app.add_handler(CallbackQueryHandler(subs_edit, pattern="^subs_edit$"))
    app.add_handler(CallbackQueryHandler(subs_edit_pick, pattern="^subs_edit_pick:"))
    app.add_handler(CallbackQueryHandler(subs_pause, pattern="^subs_pause$"))
    app.add_handler(CallbackQueryHandler(subs_daily, pattern="^subs_daily$"))
    app.add_handler(CallbackQueryHandler(lic_summary, pattern="^lic_summary$"))
    app.add_handler(CallbackQueryHandler(lic_checklist, pattern="^lic_checklist$"))
    app.add_handler(CallbackQueryHandler(lic_prazos, pattern="^lic_prazos$"))
    app.add_handler(CallbackQueryHandler(lic_docs, pattern="^lic_docs$"))
    app.add_handler(CallbackQueryHandler(lic_ask, pattern="^lic_ask$"))

    app.add_handler(CallbackQueryHandler(onboard_start, pattern="^onboard_start$"))
    app.add_handler(CallbackQueryHandler(onboard_uf, pattern="^onboard_uf:"))
    app.add_handler(CallbackQueryHandler(onboard_mun, pattern="^onboard_mun:"))
    app.add_handler(CallbackQueryHandler(onboard_mod, pattern="^onboard_mod:"))
    app.add_handler(CallbackQueryHandler(onboard_cat, pattern="^onboard_cat:"))
    app.add_handler(CallbackQueryHandler(onboard_kw, pattern="^onboard_kw:"))
    app.add_handler(CallbackQueryHandler(onboard_kw_pick, pattern="^onboard_kw_pick:"))
    app.add_handler(CallbackQueryHandler(onboard_rep, pattern="^onboard_rep:"))
    app.add_handler(CallbackQueryHandler(onboard_freq, pattern="^onboard_freq:"))
    app.add_handler(CallbackQueryHandler(onboard_delivery, pattern="^onboard_delivery:"))
    app.add_handler(CallbackQueryHandler(onboard_save, pattern="^onboard_save$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("bot_started")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
