# a-cobra

Projeto com ingestao, parse e enrichment. A forma mais estavel para usar CrewAI + Ollama aqui e rodar o CrewAI fora do Docker e apontar o app via `AGENT_URL`.

**Setup local (Windows PowerShell)**

1. Crie o arquivo `.env` a partir de `.env.example` e ajuste:

```powershell
AGENT_ENABLED=1
AGENT_URL=http://localhost:9001/enrich
OLLAMA_URL=http://localhost:11434
CREWAI_OLLAMA_MODEL=ollama_chat/llama3.2:1b
```

2. Rode o servidor CrewAI local:

```powershell
cd C:\Users\User\Desktop\a-cobra-2-\original\repo\services\crewai_ollama
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python server.py
```

3. Rode o app principal (na raiz do repo):

```powershell
cd C:\Users\User\Desktop\a-cobra-2-\original\repo
docker compose up --build
```

**Notas**

- O `AGENT_URL` precisa apontar para `http://localhost:9001/enrich`.
- O Ollama precisa estar rodando localmente em `http://localhost:11434`.

---

# Contexto Atual (Tailscale + Agent)

Data: 2026-02-08

## Locais Importantes (Windows)
- Projeto local (agent): `C:\Users\User\Desktop\crewai-ollama\`
  - Arquivo: `C:\Users\User\Desktop\crewai-ollama\server.py`
  - Venv: `C:\Users\User\Desktop\crewai-ollama\.venv\`
  - Ativação venv: `C:\Users\User\Desktop\crewai-ollama\.venv\Scripts\Activate.ps1`
- Outro `server.py` (não usar para o agent atual):
  - `C:\Users\User\Desktop\a-cobra-2-\a-cobra-2-updated-no-clone\original\repo\services\radar\notifier\server.py`

## Locais Importantes (Codespaces)
- Repo principal: `/workspaces/a-cobra-2-/original/repo`
- `.env` (AGENT_URL e TAILSCALE_AUTHKEY):
  - `/workspaces/a-cobra-2-/original/repo/.env`
- `docker-compose.yml`:
  - `/workspaces/a-cobra-2-/original/repo/docker-compose.yml`
- Logs do proxy Tailscale:
  - `docker compose logs --tail=50 tailscale-proxy`

## Resumo do Problema
- O agent (FastAPI/uvicorn) no Windows parou de responder mesmo localmente (`127.0.0.1:9001`).
- Isso quebrou o acesso via Tailscale (`100.118.114.60:9001`) e `tailscale serve` (HTTPS).
- A infraestrutura no Codespaces ficou pronta (MagicDNS + containers), mas depende do agent responder.

## Situação Atual do Windows
- `server.py` foi iniciado via venv:
  - `C:\Users\User\Desktop\crewai-ollama\server.py`
  - `uvicorn` mostrou: `running on http://0.0.0.0:9001`
- Mesmo assim, `curl http://127.0.0.1:9001/health` está **timeout**.
- Suspeita: proxy local, firewall local, ou bloqueio de loopback no Windows.
- Próximo diagnóstico (a fazer no Windows):
  1. `curl.exe -v --noproxy "*" http://127.0.0.1:9001/health`
  2. `Invoke-RestMethod http://127.0.0.1:9001/health`
  3. `netstat -ano | findstr :9001`
  4. `tasklist /FI "PID eq <PID_DO_UVICORN>"`

## Tailscale no Windows
- `tailscale serve` foi reconfigurado de volta para HTTPS:
  - `tailscale serve reset`
  - `tailscale serve --bg 9001`
  - Status: `https://desktop-n49c8ed.tail94632d.ts.net -> http://127.0.0.1:9001`
- Firewall Windows:
  - Regra aberta para 9001 (Tailscale) e 443 (Serve).
  - `Tailscale 9001` e `Tailscale 443` estão habilitadas.

## Codespaces / Docker
- MagicDNS habilitado no tailnet.
- `docker-compose.yml` atualizado para usar DNS:
  - `100.100.100.100` + `1.1.1.1` + `8.8.8.8`.
- `.env` atualizado para usar agent via proxy Tailscale interno:
  - `AGENT_URL=http://tailscale-proxy:9001/enrich?token=...`
  - `TAILSCALE_AUTHKEY` foi colocado no `.env` (precisa rotacionar depois).

## tailscale-proxy (no Docker)
- Criado serviço `tailscale-proxy`:
  - Imagem: `tailscale/tailscale:stable`
  - Usa `TS_AUTHKEY` para entrar na tailnet.
  - Roda `socat` para expor `9001` e encaminhar para `100.118.114.60:9001`.
- Logs confirmam login OK com authkey e estado Running.
- Mesmo assim, `curl` dentro de `tailscale-proxy` para `100.118.114.60:9001` dá timeout.
  - TCP abre (`nc -vz` ok), mas não recebe resposta HTTP.
  - Isso aponta para o **servidor local no Windows travado**.

## Conclusão
- O gargalo atual é **o agent no Windows**, não Tailscale/Docker.
- Enquanto `http://127.0.0.1:9001/health` não responder no Windows, nada vai funcionar fora.

## Próximos Passos Recomendados (Windows)
1. Confirmar se `curl` local está usando proxy e desabilitar:
   - `curl.exe -v --noproxy "*" http://127.0.0.1:9001/health`
2. Testar PowerShell:
   - `Invoke-RestMethod http://127.0.0.1:9001/health`
3. Se local responder:
   - Re-testar do Codespaces:
     - `docker compose exec -T api curl --max-time 10 -fsS http://tailscale-proxy:9001/health`
4. Se local NÃO responder:
   - Verificar se o processo travou (reiniciar `uvicorn`).
   - Checar firewall/proxy local.

## Observação de Segurança
- A auth key do Tailscale foi salva em `.env`.
- Após estabilizar, **revogar a key** no painel do Tailscale e gerar outra se necessário.
