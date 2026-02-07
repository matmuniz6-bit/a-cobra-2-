import re
from typing import Dict, Any, List

# regras simples (baratas) — depois você pluga por cliente
KEYWORDS = {
    "limpeza": 3,
    "manutenção": 2,
    "ti": 2,
    "informática": 2,
    "vigilância": 2,
    "saúde": 2,
    "médico": 2,
}

UF_ALVO = {"SP": 1}  # exemplo: por enquanto SP

def score_tender(t: Dict[str, Any]) -> Dict[str, Any]:
    s = 0
    reasons: List[str] = []

    obj = (t.get("objeto") or "").lower()
    for k, w in KEYWORDS.items():
        if re.search(rf"\b{re.escape(k)}\b", obj):
            s += w
            reasons.append(f"kw:{k}+{w}")

    uf = (t.get("uf") or "").upper()
    if uf in UF_ALVO:
        s += UF_ALVO[uf]
        reasons.append(f"uf:{uf}+{UF_ALVO[uf]}")

    # modalidade (exemplo)
    mod = (t.get("modalidade") or "").lower()
    if "preg" in mod:
        s += 1
        reasons.append("modalidade:pregao+1")

    return {"score_inicial": s, "reasons": reasons}
