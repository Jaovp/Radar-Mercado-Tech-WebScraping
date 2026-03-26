"""
Etapa 1 — Coleta de vagas via API pública do RemoteOK
Documentação: https://remoteok.com/api
"""

import requests
import pandas as pd
import time
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


URL_API = "https://remoteok.com/api"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

TAGS_INTERESSE = [
    "python", "data", "sql", "machine-learning", "backend",
    "django", "fastapi", "analytics", "bi", "spark", "aws",
]


def buscar_vagas_remoteok() -> list[dict]:
    print(f"[RemoteOK] Buscando vagas em {URL_API} ...")
 
    time.sleep(1)
 
    resposta = requests.get(URL_API, headers=HEADERS, timeout=15)
    resposta.raise_for_status()  # Lança erro se status != 200
 
    dados = resposta.json()
 
    # O primeiro item é sempre um aviso da API, não uma vaga
    vagas = [item for item in dados if item.get("id") != "legal"]
 
    print(f"[RemoteOK] {len(vagas)} vagas recebidas da API.")
    return vagas


def filtrar_vagas_tech(vagas: list[dict]) -> list[dict]:
    vagas_filtradas = []
    
    for vaga in vagas:
        tags_vaga = [tag.lower() for tag in vaga.get("tags", [])]
 
        # Verifica se alguma tag de interesse aparece nas tags da vaga
        tem_tag_interesse = any(tag in tags_vaga for tag in TAGS_INTERESSE)
 
        if tem_tag_interesse:
            vagas_filtradas.append(vaga)
 
    print(f"[RemoteOK] {len(vagas_filtradas)} vagas filtradas para área de tech.")
    return vagas_filtradas


def normalizar_para_dataframe(vagas: list[dict]) -> pd.DataFrame: 
    registros = []
 
    for vaga in vagas:
        registros.append({
            "id":           vaga.get("id"),
            "fonte":        "RemoteOK",
            "cargo":        vaga.get("position"),
            "empresa":      vaga.get("company"),
            "local":        vaga.get("location", "Remote"),
            "tags":         ", ".join(vaga.get("tags", [])),
            "salario_min":  vaga.get("salary_min"),   # pode ser None
            "salario_max":  vaga.get("salary_max"),   # pode ser None
            "url":          vaga.get("url"),
            "data_coleta":  datetime.now().strftime("%Y-%m-%d"),
        })
 
    df = pd.DataFrame(registros)
    print(f"[RemoteOK] DataFrame criado: {df.shape[0]} linhas x {df.shape[1]} colunas.")
    return df

def salvar_dados_brutos(df: pd.DataFrame, caminho: str) -> None:
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"[RemoteOK] Dados salvos em: {caminho}")


if __name__ == "__main__":
    # 1. Busca todas as vagas da API
    vagas_brutas = buscar_vagas_remoteok()
 
    # 2. Filtra só as de tech
    vagas_tech = filtrar_vagas_tech(vagas_brutas)
 
    # 3. Converte para DataFrame
    df_remoteok = normalizar_para_dataframe(vagas_tech)
 
    # 4. Salva os dados brutos
    salvar_dados_brutos(df_remoteok, ROOT / "data" / "raw" / "vagas_remoteok.csv")
 
    # 5. Preview no terminal
    print("\n── Preview dos dados ──")
    print(df_remoteok[["cargo", "empresa", "local", "tags", "salario_min"]].head(10).to_string())