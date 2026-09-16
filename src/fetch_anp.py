import requests
import pandas as pd
import json
import os
import unicodedata
from datetime import datetime

URLS = {
    "2026": "https://www.gov.br/anp/pt-br/assuntos/importacoes-e-exportacoes/arquivos-desembaracos/desembaraco-2026.xlsx",
    "2025": "https://www.gov.br/anp/pt-br/assuntos/importacoes-e-exportacoes/arquivos-desembaracos/desembaraco-2025.xlsx",
}

NCM_DESC = {
    "27090010": "Petróleo bruto", "27090090": "Petróleo bruto (outros)",
    "27101921": "Óleo diesel", "27101922": "Diesel marítimo",
    "27111100": "GNL", "27111200": "Propano", "27111300": "Butano",
    "27111900": "Outros GLP", "27112100": "Gás natural (gasoso)",
    "27101941": "Querosene / JET A-1", "27101942": "Querosene iluminante",
    "27101931": "Óleo combustível", "38260010": "Biodiesel",
    "38260090": "Biodiesel (misturas)", "27101951": "Lubrificantes",
    "27101112": "Gasolina", "27101113": "Gasolina aviação",
}

CAT_MAP = {
    "Petróleo bruto": ["27090010", "27090090"],
    "GNL / Gás natural": ["27111100", "27112100"],
    "GLP": ["27111200", "27111300", "27111900"],
    "Diesel": ["27101921", "27101922"],
    "Querosene / JET": ["27101941", "27101942"],
    "Óleo combustível": ["27101931"],
    "Biodiesel": ["38260010", "38260090"],
    "Lubrificantes": ["27101951"],
    "Gasolina": ["27101112", "27101113"],
}

FX_USD = {
    "27090010": 0.62, "27090090": 0.62, "27101921": 0.85, "27101922": 0.85,
    "27111100": 0.48, "27111200": 0.55, "27111300": 0.55, "27111900": 0.55,
    "27112100": 0.38, "27101941": 1.1, "27101942": 0.9, "27101931": 0.58,
    "38260010": 0.72, "38260090": 0.72, "27101951": 1.4,
    "27101112": 0.95, "27101113": 1.05,
}

MOEDA = {
    "ARABIA SAUDITA": "USD", "ESTADOS UNIDOS": "USD", "QATAR": "USD",
    "NIGERIA": "USD", "IRAQUE": "USD", "EAU": "USD", "ANGOLA": "USD",
    "REINO UNIDO": "USD/GBP", "RUSSIA": "USD", "TRINIDAD E TOBAGO": "USD",
    "HOLANDA": "USD/EUR", "ARGENTINA": "USD", "BOLIVIA": "USD",
    "NORUEGA": "USD/NOK", "MEXICO": "USD", "VENEZUELA": "USD",
    "KUWAIT": "USD", "LIBIA": "USD", "EQUADOR": "USD",
    "AZERBAIJAO": "USD", "CAZAQUISTAO": "USD", "OMA": "USD",
    "ALEMANHA": "USD/EUR", "BELGICA": "USD/EUR", "FRANCA": "USD/EUR",
    "COLOMBIA": "USD", "PERU": "USD", "CHILE": "USD",
}

def norm(texto):
    """Remove acentos e retorna em maiúsculo sem acentos."""
    txt = str(texto).strip().upper()
    return unicodedata.normalize("NFKD", txt).encode("ASCII", "ignore").decode("ASCII")

def get_cat(ncm):
    for cat, ncms in CAT_MAP.items():
        if str(ncm) in ncms:
            return cat
    return "Outros"

def fx_est(ncm, kg):
    return round(float(kg) * FX_USD.get(str(ncm), 0.6))

def download_excel(year):
    url = URLS.get(str(year))
    if not url:
        return None
    print(f"Baixando {year}...")
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ANP-Monitor/1.0)"}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    path = f"/tmp/desembaraco-{year}.xlsx"
    with open(path, "wb") as f:
        f.write(r.content)
    print(f"  Salvo: {len(r.content)//1024} KB")
    return path

def parse_excel(path):
    xl = pd.ExcelFile(path)
    sheet = xl.sheet_names[0]
    print(f"  Aba: {sheet}")

    # O Excel da ANP tem sempre: linha 0=título, linha 1=vazia, linha 2=cabeçalho
    # Lê com header=2 diretamente
    df = pd.read_excel(path, sheet_name=sheet, header=2)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  Colunas: {list(df.columns)}")

    # Mapeamento fixo baseado nos nomes reais da ANP
    # Colunas conhecidas: 'Mês de desembaraço', 'Importador', 'CNPJ', 'UF DO CNPJ*',
    #                     'NCM', 'Descrição NCM', 'UA Despacho', 'Pais de origem',
    #                     'Quantidade de produto em quilos'
    col_map = {}
    for col in df.columns:
        c = norm(col)
        if c == "IMPORTADOR":
            col_map["empresa"] = col
        elif c == "CNPJ":
            col_map["cnpj"] = col
        elif c == "NCM":
            col_map["ncm"] = col
        elif "QUILOS" in c or "QUANTIDADE" in c:
            col_map["kg"] = col
        elif "PAIS" in c and "ORIGEM" in c:
            col_map["pais"] = col
        elif "UA" in c and "DESPACHO" in c:
            col_map["ua"] = col
        elif "MES" in c or "MÊS" in c:
            col_map["mes"] = col

    print(f"  Mapeamento: {col_map}")

    if len(col_map) < 4:
        print(f"  ERRO: apenas {len(col_map)} colunas mapeadas. Pulando.")
        return []

    records = []
    skip = {"NAN", "", "NONE", "IMPORTADOR"}

    for _, row in df.iterrows():
        emp = str(row.get(col_map.get("empresa", ""), "")).strip()
        if not emp or norm(emp) in skip:
            continue

        ncm_raw = str(row.get(col_map.get("ncm", ""), "")).strip()
        ncm = ncm_raw.replace(".", "").replace(" ", "")
        if not ncm.isdigit() or len(ncm) < 6:
            continue

        try:
            kg = float(str(row.get(col_map.get("kg", ""), 0)).replace(",", ".").replace(" ", ""))
        except:
            kg = 0
        if kg <= 0:
            continue

        pais = norm(row.get(col_map.get("pais", ""), ""))
        ua   = norm(row.get(col_map.get("ua", ""), ""))
        cnpj = str(row.get(col_map.get("cnpj", ""), "")).strip()
        mes  = str(row.get(col_map.get("mes", ""), "")).strip()

        records.append({
            "empresa": emp,
            "cnpj": cnpj,
            "ncm": ncm,
            "ncm_desc": NCM_DESC.get(ncm, f"NCM {ncm}"),
            "categoria": get_cat(ncm),
            "kg": kg,
            "pais": pais,
            "ua": ua,
            "mes": mes,
            "moeda": MOEDA.get(pais, "USD"),
            "fx_est": fx_est(ncm, kg),
        })

    print(f"  {len(records)} registros válidos")
    if records:
        print(f"  Exemplo: {records[0]}")
    return records

def main():
    os.makedirs("data", exist_ok=True)
    all_records = []
    meta = {"updated_at": datetime.utcnow().isoformat() + "Z", "years": {}}

    for year in ["2026", "2025"]:
        try:
            path = download_excel(year)
            if not path:
                continue
            records = parse_excel(path)
            all_records.extend(records)
            meta["years"][year] = {
                "records": len(records),
                "empresas": len(set(r["empresa"] for r in records)),
                "total_kg": sum(r["kg"] for r in records),
                "total_fx": sum(r["fx_est"] for r in records),
            }
            with open(f"data/records_{year}.json", "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  {year}: {len(records)} registros salvos")
        except Exception as e:
            import traceback
            print(f"  Erro em {year}: {e}")
            traceback.print_exc()
            meta["years"][year] = {"error": str(e)}

    with open("data/meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"Concluído: {datetime.utcnow().isoformat()} — {len(all_records)} registros totais")

if __name__ == "__main__":
    main()
