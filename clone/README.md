# a-cobra

## Rodar local (sem Docker, mais simples)

1. Clonar o repo

```bash
git clone https://github.com/matmuniz6-bit/a-cobra-2-.git
cd a-cobra-2-/clone
```

2. Criar venv e instalar dependencias

```bash
python -m venv .venv
# PowerShell
.\.venv\Scripts\Activate.ps1
# Linux/Mac
# source .venv/bin/activate

pip install -U pip
pip install -r services/api/requirements.txt
pip install -r services/bot/requirements.txt
pip install -r services/radar/notifier/requirements.txt
```

3. Apontar para o Ollama local

O Ollama rodando no seu PC sempre fica em:

```
http://localhost:11434
```

Defina no `.env` (ou na sua sessao):

```
OLLAMA_URL=http://localhost:11434
OLLAMA_CHAT_MODEL=llama3.1
OLLAMA_EMBED_MODEL=nomic-embed-text
EMBEDDINGS_ENABLED=1
```

4. Rodar a API (exemplo)

```bash
python -m services.api.app.main
```

## Rodar com Docker

1. Copie `.env.example` para `.env` e ajuste valores
2. Suba os servicos

```bash
docker compose up --build
```

Se quiser usar Ollama com Docker no seu PC, troque no `.env`:

```
OLLAMA_URL=http://host.docker.internal:11434
```

## Observacoes

- Neste ambiente (Codespace), nao da para usar o Ollama do seu PC.
- Para usar CrewAI, configure `AGENT_URL` e `AGENT_ENABLED=1`.
