# Radar Mercado Tech

Projeto de web scraping para aprendizado — coleta, trata e analisa vagas de emprego na área de tecnologia.

## Stack
`Python` • `requests` • `BeautifulSoup` • `pandas` • `numpy` • `Power BI`

## Etapas
| Etapa | Descrição | Status |
|-------|-----------|--------|
| 1 | Coleta de dados (API + Scraping) | ✅ |
| 2 | ETL e limpeza | ✅ |
| 3 | EDA | 🔜 |
| 4 | Dashboard Power BI | 🔜 |

## Como rodar

```bash
pip install -r requirements.txt
python src/coletar_dados.py
```

## Fontes de dados
- **RemoteOK** — API pública (JSON)
- **Vagas.com.br** — Scraping HTML (robots.txt verificado)