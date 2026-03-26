"""
Etapa 1 — Coleta de vagas via scraping HTML do Vagas.com.br
robots.txt verificado: rotas de vagas estão liberadas para crawlers.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Termos de busca
TERMOS_BUSCA = [
    "python",
    "data-scientist",
    "analista-de-dados",
    "engenheiro-de-dados",
    "cientista-de-dados",
    "machine-learning",
    "sql",
]

# Quantas páginas buscar
MAX_PAGINAS= 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9",
}


PAUSA_ENTRE_REQUEST = 2  # segundos


def montar_url(termo:str, pagina:int) -> str:
    """Montar a URL de busca para o Vagas.com.br"""
    return f"https://www.vagas.com.br/vagas-de-{termo}?pagina={pagina}"

def baixar_pagina(url: str) -> BeautifulSoup | None:
    try:
        time.sleep(PAUSA_ENTRE_REQUEST)
        resposta = requests.get(url, headers=HEADERS, timeout=15)
        resposta.raise_for_status()
        return BeautifulSoup(resposta.text, "lxml")
    except requests.RequestException as e:
        print(f"Erro ao baixar {url}: {e}")
        return None
    
def extrair_vagas_da_pagina(soup: BeautifulSoup, termo: str) -> list[dict]:
    vagas = []

    cards = soup.find_all("li", class_="vaga")

    for card in cards:
        try:
            tag_cargo = card.find("a", class_="link-detalhes-vaga")
            cargo = tag_cargo.get_text(strip=True) if tag_cargo else None

            tag_empresa = card.find("span", class_="emprVaga")
            empresa = tag_empresa.get_text(strip=True) if tag_empresa else None

            tag_local = card.find("span", class_="local")
            local = tag_local.get_text(strip=True) if tag_local else None

            tag_data = card.find("span", class_="data-publicacao")
            data_publicacao = tag_data.get_text(strip=True) if tag_data else None

            url_vaga = None
            if tag_cargo and tag_cargo.get("href"):
                url_vaga = "https://www.vagas.com.br" + tag_cargo["href"]

            vagas.append({
                "id":              url_vaga,  # URL como identificador único
                "fonte":           "Vagas.com.br",
                "cargo":           cargo,
                "empresa":         empresa,
                "local":           local,
                "tags":            termo,     
                "salario_min":     None,      # Vagas.com.br raramente exibe salário
                "salario_max":     None,
                "url":             url_vaga,
                "data_publicacao": data_publicacao,
                "data_coleta":     datetime.now().strftime("%Y-%m-%d"),
            })

        except Exception as e:
            print(f"Erro ao extrair vaga: {e}")
            continue

    return vagas

def coletar_todas_as_vagas() ->pd.DataFrame:
    todos_os_registros = []

    for termo in TERMOS_BUSCA:
        print(f"\n[Vagas.com.br] Buscando: '{termo}'")

        for pagina in range(1, MAX_PAGINAS + 1):
            url = montar_url(termo, pagina)
            print(f"  Página {pagina}: {url}")

            soup = baixar_pagina(url)
            if soup is None:
                break

            vagas_pagina = extrair_vagas_da_pagina(soup, termo)

            if not vagas_pagina:
                print(f"  Nenhuma vaga encontrada nesta página {pagina}.")
                break

            todos_os_registros.extend(vagas_pagina)
            print(f"  Vagas coletadas nesta página: {len(vagas_pagina)}")

    df = pd.DataFrame(todos_os_registros)

    # Remove duplicatas pelo URL da vaga 
    antes= len(df)
    df = df.drop_duplicates(subset="url").reset_index(drop=True)
    depois = len(df)
    print(f"\n[Vagas.com.br] Duplicatas removidas: {antes - depois}")
    print(f"[Vagas.com.br] Total final: {depois} vagas.")

    return df


def salvar_dados_brutos(df: pd.DataFrame, caminho: str) -> None:
    """Salva o CSV de dados brutos na pasta data/raw."""
    df.to_csv(caminho, index=False, encoding="utf-8-sig")
    print(f"[Vagas.com.br] Dados salvos em: {caminho}")


if __name__ == "__main__":
    # 1. Coletar vagas
    df_vagas = coletar_todas_as_vagas()

    # 2. Salvar dados brutos
    salvar_dados_brutos(df_vagas, ROOT / "data" / "raw" / "vagas_com_br.csv")

    # 3. Preview dos dados
    print("\n-- Previw dos dados --")
    print(df_vagas[["cargo", "empresa", "local", "data_publicacao"]].head(10).to_string())