"""
Etapa 1 — Script principal de coleta
Executa os dois scrapers e combina os dados em um único CSV.
 
Como rodar:
    cd radar-mercado-tech
    python src/coletar_dados.py
"""

import pandas as pd
from pathlib import Path
from remoteok_scraper import buscar_vagas_remoteok, filtrar_vagas_tech, normalizar_para_dataframe, salvar_dados_brutos as salvar_remoteok
from vagas_scraper import coletar_todas_as_vagas, salvar_dados_brutos as salvar_vagas

ROOT = Path(__file__).resolve().parent.parent

def main():
    print("=" * 50)
    print("Iniciando coleta de dados...")
    print("=" * 50)

    # -- Fonte 1: RemoteOK --
    print("\n[RemoteOK] Coletando vagas...")
    print("-" * 40)
    vagas_brutas = buscar_vagas_remoteok()
    vagas_tech = filtrar_vagas_tech(vagas_brutas)
    df_remoteok = normalizar_para_dataframe(vagas_tech)
    salvar_remoteok(df_remoteok, "data/raw/remoteok_vagas.csv")

    # -- Fonte 2: Vagas.com.br --
    print("\n[Vagas.com.br] Coletando vagas...")
    print("-" * 40)
    df_vagas_br = coletar_todas_as_vagas()
    salvar_vagas(df_vagas_br, "data/raw/vagas_com_br.csv")

    print("\nColeta de dados concluída com sucesso!")
    print("=" * 50)

    print("\n COMBINANDO OS DATASETS...")
    print("-" * 40)

    df_combinado = pd.concat([df_remoteok, df_vagas_br], ignore_index=True)

    # Remove duplicatas pelo URL
    df_combinado = df_combinado.drop_duplicates(subset="url").reset_index(drop=True)
    
    caminho_combinado = "data/raw/vagas_combinadas.csv"
    df_combinado.to_csv(caminho_combinado, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 55)
    print("  COLETA CONCLUÍDA")
    print("=" * 55)
    print(f"  RemoteOK:       {len(df_remoteok):>5} vagas")
    print(f"  Vagas.com.br:   {len(df_vagas_br):>5} vagas")
    print(f"  Total combinado:{len(df_combinado):>5} vagas")
    print(f"\n  Arquivos salvos em data/raw/")
    print(f"  → vagas_remoteok.csv")
    print(f"  → vagas_vagas_com_br.csv")
    print(f"  → vagas_combinado.csv  ✅ (usar na Etapa 2)")
    print("=" * 55)
 
 
if __name__ == "__main__":
    main()
