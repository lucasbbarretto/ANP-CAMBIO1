import requests
import pandas as pd
import json
import os
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
    "ARÁBIA SAUDITA": "USD", "ESTADOS UNIDOS": "USD", "QATAR": "USD",
    "NIGÉRIA": "USD", "IRAQUE": "USD", "EAU": "USD", "ANGOLA": "USD",
    "REINO UNIDO": "USD/GBP", "RÚSSIA": "USD", "TRINIDAD E TOBAGO": "USD",
    "HOLANDA": "USD/EUR", "ARGENTINA": "USD", "BOLÍVIA": "USD",
    "NORUEGA": "USD/NOK", "MÉXICO": "USD", "VENEZUELA": "USD",
}

def get_cat(ncm):
    ncm = str(ncm)
    for cat, ncms in CAT_MAP.items():
        if ncm in ncms:
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
    print(f"  Salvo em {path} ({len(r.content)//1024} KB)")
    return path

def parse_excel(path):
    xl = pd.ExcelFile(path)
    print(f"  Abas encontradas: {xl.sheet_names}")
    df = xl.parse(xl.sheet_names[0])
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  Colunas: {list(df.columns)}")
    print(f"  Shape: {df.shape}")
    col_map = {}
    for col in df.columns:
        c = col.lower()
        if any(x in c for x in ["importador", "razão", "empresa", "nome"]):
            col_map["empresa"] = col
        elif "cnpj" in c:
            col_map["cnpj"] = col
        elif "ncm" in c:
            col_map["ncm"] = col
        elif any(x in c for x in ["quilos", "kg", "quantidade", "peso"]):
            col_map["kg"] = col
        elif any(x in c for x in ["país", "pais", "origem"]):
            col_map["pais"] = col
        elif any(x in c for x in ["unidade", "ua", "adm", "porto", "despacho"]):
            col_map["ua"] = col
        elif any(x in c for x in ["mês", "mes", "referência", "referencia", "período"]):
            col_map["mes"] = col
    print(f"  Mapeamento: {col_map}")
    records = []
    for _, row in df.iterrows():
        emp = str(row.get(col_map.get("empresa", ""), "")).strip()
        if not emp or emp.lower() in ["nan", "", "none"]:
            continue
        ncm = str(row.get(col_map.get("ncm", ""), "")).strip().replace(".", "")
        try:
            kg = float(str(row.get(col_map.get("kg", ""), 0)).replace(",", "."))
        except:
            kg = 0
        pais = str(row.get(col_map.get("pais", ""), "")).strip().upper()
        ua = str(row.get(col_map.get("ua", ""), "")).strip().upper()
        cnpj = str(row.get(col_map.get("cnpj", ""), "")).strip()
        mes = str(row.get(col_map.get("mes", ""), "")).strip()
        records.append({
            "empresa": emp, "cnpj": cnpj, "ncm": ncm,
            "ncm_desc": NCM_DESC.get(ncm, f"NCM {ncm}"),
            "categoria": get_cat(ncm), "kg": kg, "pais": pais,
            "ua": ua, "mes": mes, "moeda": MOEDA.get(pais, "USD"),
            "fx_est": fx_est(ncm, kg),
        })
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
            print(f"  {year}: {len(records)} registros")
            all_records.extend(records)
            meta["years"][year] = {
                "records": len(records),
                "empresas": len(set(r["empresa"] for r in records)),
                "total_kg": sum(r["kg"] for r in records),
                "total_fx": sum(r["fx_est"] for r in records),
            }
            with open(f"data/records_{year}.json", "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"  Erro em {year}: {e}")
            meta["years"][year] = {"error": str(e)}
    with open("data/meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"Concluído: {datetime.utcnow().isoformat()}")

if __name__ == "__main__":
    main()
