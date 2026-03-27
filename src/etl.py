"""
Etapa 2 — ETL: limpeza e transformação dos dados brutos
Input:  data/raw/vagas_combinado.csv
Output: data/processed/vagas_tratado.csv
"""
 
import pandas as pd
import numpy as np
import requests
import re
from datetime import datetime
from pathlib import Path
 
ROOT = Path(__file__).resolve().parent.parent

SKILLS_CONHECIDAS = [
    "python", "sql", "r", "scala", "java", "javascript",
    "aws", "azure", "gcp",
    "spark", "hadoop", "kafka", "airflow",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch",
    "power bi", "tableau", "looker", "metabase",
    "dbt", "docker", "kubernetes", "git",
    "machine learning", "deep learning", "nlp", "computer vision",
    "django", "fastapi", "flask",
]

MAPA_CARGOS = {
    # Dados — PT + EN
    r"(data\s*scien|cientista\s*de\s*dado)":                      "Data Scientist",
    r"(data\s*engineer|engenheiro\s*de\s*dado|data\s*infra)":     "Data Engineer",
    r"(data\s*anal|analista\s*de\s*dado|business\s*anal)":        "Data Analyst",
    r"(machine\s*learning|ml\s*engineer|ai\s*engineer)":          "ML Engineer",
    r"(bi\s*dev|business\s*intel|analista\s*de\s*bi)":            "BI Developer",
    # Engenharia — PT + EN
    r"(backend|back.end|back\s*end|api\s*dev|rails|django\s*dev|node\.?js\s*dev)": "Backend Developer",
    r"(frontend|front.end|front\s*end|react\s*dev|vue\s*dev|angular\s*dev)":       "Frontend Developer",
    r"(fullstack|full.stack|full\s*stack)":                       "Fullstack Developer",
    r"(devops|sre|site\s*reliab|platform\s*eng|infra\s*eng)":    "DevOps/SRE",
    r"(mobile|android|ios|flutter|react\s*native)":               "Mobile Developer",
    # Engenharia geral (EN)
    r"(software\s*eng|software\s*dev|senior\s*eng|staff\s*eng|principal\s*eng|"
    r"lead\s*eng|engineer(?!\s*(data|ml|ai|devops|infra|platform|security|cloud)))": "Software Engineer",
    # Outros tech — PT + EN
    r"(qa\b|quality\s*assur|tester|teste\s*de\s*soft)":           "QA Engineer",
    r"(product\s*manager|gerente\s*de\s*produto|product\s*owner)":"Product Manager",
    r"(ux|ui\s*design|product\s*design|designer)":                "UX/UI Designer",
    r"(cloud\s*arch|cloud\s*eng|infra|infrastructure\s*eng)":     "Cloud/Infra",
    r"(security|segurança|cybersec|appsec|pentest)":              "Security",
}

MAPA_MODELO = {
    r"remot": "Remoto",
    r"híbrid|hibrid": "Híbrido",
    r"presenci|on.?site|in.?office": "Presencial",
}

MOEDA_POR_FONTE = {
    "RemoteOK":    "USD",
    "Vagas.com.br": "BRL",
}


# ── Funções de limpeza ──────────────────────────────────────────────────────────
 
def buscar_cotacao_usd_brl() -> float:
    """
    Estratégia de fallback em cascata:
      1. AwesomeAPI  — endpoint simples, retorna cotação atual imediatamente
      2. Banco Central (CotacaoDolarDia) — cotação oficial do dia (formato MM-DD-YYYY)
      3. Valor fixo hardcoded — apenas se todas as APIs falharem
    """
    FALLBACK = 5.70
 
    # ── 1. AwesomeAPI ─────────────────
    try:
        url = "https://economia.awesomeapi.com.br/last/USD-BRL"
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        cotacao = float(resp.json()["USDBRL"]["bid"])
        print(f"[ETL] Cotação USD/BRL (AwesomeAPI): R$ {cotacao:.2f}")
        return cotacao
    except Exception as e:
        print(f"[ETL] AwesomeAPI falhou ({e}). Tentando Banco Central...")
 
    # ── 2. BCB — CotacaoDolarDia (formato MM-DD-YYYY) ────────
    try:
        hoje = datetime.now().strftime("%m-%d-%Y")
        url = (
            "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoDolarDia(dataCotacao=@dataCotacao)"
            f"?@dataCotacao='{hoje}'&$format=json"
        )
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        dados = resp.json().get("value", [])
 
        if dados:
            cotacao = float(dados[-1]["cotacaoVenda"])
            print(f"[ETL] Cotação USD/BRL (BCB — dia): R$ {cotacao:.2f}")
            return cotacao
 
        # Sem dados = fim de semana ou feriado, tenta o período
        raise ValueError("Sem cotação para hoje (feriado/fim de semana).")
 
    except Exception as e:
        print(f"[ETL] BCB/dia falhou ({e}). Tentando período...")
 
    # ── 3. Fallback fixo ──────────────────────────────────────────────────────
    print(f"[ETL] Todas as APIs falharam. Usando fallback fixo: R$ {FALLBACK:.2f}")
    return FALLBACK
 
def carregar_dados(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho, encoding="utf-8-sig")
    print(f"[ETL] Carregado: {len(df)} linhas, {df.shape[1]} colunas.")
    return df
 
 
def remover_duplicatas_e_nulos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
 
    # Remove duplicatas pelo URL
    df = df.drop_duplicates(subset="url").reset_index(drop=True)
 
    # Remove linhas sem cargo (campo obrigatório)
    df = df.dropna(subset=["cargo"]).reset_index(drop=True)
 
    depois = len(df)
    print(f"[ETL] Duplicatas/nulos removidos: {antes - depois} linhas. Restam: {depois}.")
    return df
 
 
def normalizar_texto(texto: str) -> str:
    if pd.isna(texto):
        return ""
    return re.sub(r"\s+", " ", str(texto).lower().strip()) # remove espaços extras e normaliza para minúsculas
 
 
def categorizar_cargo(cargo: str) -> str:
    cargo_norm = normalizar_texto(cargo)
    for padrao, categoria in MAPA_CARGOS.items():
        if re.search(padrao, cargo_norm):
            return categoria
    return "Outros"
 
 
def normalizar_cargos(df: pd.DataFrame) -> pd.DataFrame:
    df["cargo_categoria"] = df["cargo"].apply(categorizar_cargo)
    contagem = df["cargo_categoria"].value_counts()
    print(f"[ETL] Categorias de cargo criadas:\n{contagem.to_string()}\n")
    return df
 
 
def extrair_skills(texto_tags: str) -> str:
    if pd.isna(texto_tags):
        return ""
 
    texto_norm = normalizar_texto(texto_tags)
    skills_encontradas = []
 
    for skill in SKILLS_CONHECIDAS:
        padrao = r"\b" + re.escape(skill) + r"\b"
        if re.search(padrao, texto_norm):
            skills_encontradas.append(skill)
 
    return " | ".join(skills_encontradas)
 
 
def extrair_skills_coluna(df: pd.DataFrame) -> pd.DataFrame:
    # Combina tags + cargo para aumentar a detecção
    texto_combinado = df["tags"].fillna("") + " " + df["cargo"].fillna("")
    df["skills_detectadas"] = texto_combinado.apply(extrair_skills)
 
    total_com_skill = (df["skills_detectadas"] != "").sum()
    print(f"[ETL] Vagas com ao menos 1 skill detectada: {total_com_skill} de {len(df)}.")
    return df
 
 
def detectar_modelo_trabalho(texto: str) -> str:
    if pd.isna(texto):
        return "Não informado"
 
    texto_norm = normalizar_texto(texto)
    for padrao, modelo in MAPA_MODELO.items():
        if re.search(padrao, texto_norm):
            return modelo
 
    return "Não informado"
 
 
def normalizar_modelo_trabalho(df: pd.DataFrame) -> pd.DataFrame:
    # Primeiro tenta detectar pelo campo local via regex
    df["modelo_trabalho"] = df["local"].apply(detectar_modelo_trabalho)
 
    # RemoteOK é um board 100% remoto
    mask_remoteok_sem_info = (
        (df["fonte"] == "RemoteOK") &
        (df["modelo_trabalho"] == "Não informado")
    )
    df.loc[mask_remoteok_sem_info, "modelo_trabalho"] = "Remoto"
 
    print(f"[ETL] Modelos de trabalho:\n{df['modelo_trabalho'].value_counts().to_string()}\n")
    return df
 
 
def tratar_salarios(df: pd.DataFrame, cotacao_usd: float) -> pd.DataFrame:

    df["salario_min"] = pd.to_numeric(df["salario_min"], errors="coerce")
    df["salario_max"] = pd.to_numeric(df["salario_max"], errors="coerce")
 
    # RemoteOK usa 0 para "não informado" — trata como NaN
    df.loc[df["fonte"] == "RemoteOK", "salario_min"] = df.loc[
        df["fonte"] == "RemoteOK", "salario_min"
    ].replace(0, np.nan)
    df.loc[df["fonte"] == "RemoteOK", "salario_max"] = df.loc[
        df["fonte"] == "RemoteOK", "salario_max"
    ].replace(0, np.nan)
 
    # Identifica a moeda pela coluna 'fonte'
    df["moeda"] = df["fonte"].map(MOEDA_POR_FONTE).fillna("BRL")
 
    # ── NumPy entra aqui ─────────────────────────────────────────────────────
 
    sal_min  = df["salario_min"].to_numpy()
    sal_max  = df["salario_max"].to_numpy()
    eh_usd   = (df["moeda"] == "USD").to_numpy()
 
    # Calcula salário médio anual por vaga na moeda original
    sal_medio_anual = np.where(
        ~np.isnan(sal_min) & ~np.isnan(sal_max),
        (sal_min + sal_max) / 2,
        np.nan,
    )
 
    # RemoteOK = anual → divide por 12 para mensal
    # Vagas.com.br = já é mensal → mantém
    sal_medio_mensal = np.where(eh_usd, sal_medio_anual / 12, sal_medio_anual)
 
    # Converte USD mensal → BRL mensal
    sal_medio_brl = np.where(eh_usd, sal_medio_mensal * cotacao_usd, sal_medio_mensal)
 
    df["salario_medio_brl"] = np.where(np.isnan(sal_medio_anual), np.nan, sal_medio_brl)
 
    # Estatísticas globais separadas por moeda (para conferência)
    for moeda, label in [("USD", "RemoteOK (USD anual → BRL mensal)"), ("BRL", "Vagas.com.br (BRL mensal)")]:
        mask      = (df["moeda"] == moeda).to_numpy()
        arr       = df["salario_medio_brl"].to_numpy()
        arr_moeda = np.where(mask, arr, np.nan)
        n         = np.sum(~np.isnan(arr_moeda))
 
        if n > 0:
            p25, p75 = np.nanpercentile(arr_moeda[~np.isnan(arr_moeda)], [25, 75])
            print(f"[ETL] Salários — {label}: {n} vagas")
            print(f"      Média:    R$ {np.nanmean(arr_moeda):>10,.0f} /mês")
            print(f"      Mediana:  R$ {np.nanmedian(arr_moeda):>10,.0f} /mês")
            print(f"      P25/P75:  R$ {p25:,.0f} / R$ {p75:,.0f}\n")
        else:
            print(f"[ETL] {label}: nenhuma vaga com salário informado.\n")
 
    return df
 
 
def limpar_local(df: pd.DataFrame) -> pd.DataFrame:
    df["local"] = df["local"].apply(
        lambda x: re.sub(r"\s+", " ", str(x).strip()) if pd.notna(x) else "Não informado"
    )
    return df
 
 
def adicionar_contagem_skills(df: pd.DataFrame) -> pd.DataFrame:
    df["qtd_skills"] = df["skills_detectadas"].apply(
        lambda x: len(x.split(" | ")) if x else 0
    )
    return df

# ────────────────────────────────────────────────────────────────────────

def executar_etl() -> pd.DataFrame:
    print("=" * 55)
    print("  RADAR MERCADO TECH — ETAPA 2: ETL")
    print("=" * 55 + "\n")
 
    caminho_input  = ROOT / "data" / "raw"       / "vagas_combinadas.csv"
    caminho_output = ROOT / "data" / "processed" / "vagas_tratado.csv"
 
    # 1. Carregar
    df = carregar_dados(caminho_input)
 
    # 2. Remover duplicatas e nulos
    df = remover_duplicatas_e_nulos(df)
 
    # 3. Limpar campo local
    df = limpar_local(df)
 
    # 4. Normalizar cargos → cargo_categoria
    df = normalizar_cargos(df)
 
    # 5. Extrair skills das tags
    df = extrair_skills_coluna(df)
 
    # 6. Detectar modelo de trabalho
    df = normalizar_modelo_trabalho(df)
 
    # 7. Buscar cotação e tratar salários
    cotacao_usd = buscar_cotacao_usd_brl()
    df = tratar_salarios(df, cotacao_usd)
 
    # 8. Adicionar contagem de skills
    df = adicionar_contagem_skills(df)
 
    # 9. Salvar
    caminho_output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(caminho_output, index=False, encoding="utf-8-sig")
 
    print("=" * 55)
    print("  ETL CONCLUÍDO")
    print("=" * 55)
    print(f"  Linhas finais: {len(df)}")
    print(f"  Colunas:       {list(df.columns)}")
    print(f"\n  Arquivo salvo em: data/processed/vagas_tratado.csv  ✅")
    print("=" * 55)
 
    return df
 
 
if __name__ == "__main__":
    df_tratado = executar_etl()
 
    print("\n── Preview ──")
    print(df_tratado[[
        "cargo_categoria", "empresa", "local",
        "modelo_trabalho", "skills_detectadas", "moeda", "salario_medio_brl"
    ]].head(10).to_string())