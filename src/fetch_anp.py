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
}

def norm(texto):
    """Remove acentos e normaliza para maiúsculo."""
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

def find_header_row(df_raw):
    """Encontra linha do cabeçalho procurando por palavras-chave nas colunas."""
    keywords = ["importador", "cnpj", "ncm", "quilos", "pais", "unidade", "origem", "quantidade"]
    best_row = None
    best_hits = 0
    for i, row in df_raw.iterrows():
        if i > 20:  # cabeçalho sempre nas primeiras linhas
            break
        row_str = " ".join(norm(str(v)) for v in row.values)
        hits = sum(1 for k in keywords if k in row_str)
        if hits > best_hits:
            best_hits = hits
            best_row = i
    if best_row is not None and best_hits >= 2:
        print(f"  Cabeçalho na linha {best_row} ({best_hits} hits)")
        return best_row
    return None

def map_columns(df):
    """Mapeia colunas pelos nomes reais da ANP."""
    col_map = {}
    for col in df.columns:
        c = norm(col)
        if any(x in c for x in ["importador", "razao social", "razao"]):
            col_map.setdefault("empresa", col)
        if "cnpj" in c:
            col_map.setdefault("cnpj", col)
        if "ncm" in c and "desc" not in c and "nome" not in c and "sigla" not in c:
            col_map.setdefault("ncm", col)
        if any(x in c for x in ["quilos", "quilo", "quantidade de produto"]):
            col_map.setdefault("kg", col)
        if any(x in c for x in ["pais de origem", "pais orig", "origem"]):
            col_map.setdefault("pais", col)
        if any(x in c for x in ["ua despacho", "unidade adm", "ua ", " ua"]):
            col_map.setdefault("ua", col)
        if any(x in c for x in ["mes de desembaraco", "mes de", "periodo", "referencia"]):
            col_map.setdefault("mes", col)
    return col_map

def parse_excel(path):
    xl = pd.ExcelFile(path)
    sheet = xl.sheet_names[0]
    print(f"  Aba: {sheet}")

    # Lê sem cabeçalho para detectar a linha correta
    df_raw = xl.parse(sheet, header=None)
    header_row = find_header_row(df_raw)

    if header_row is None:
        # Tenta ler com header=0 e verifica se o mapeamento faz sentido
        print("  Tentando header=0...")
        header_row = 0

    df = xl.parse(sheet, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"  Colunas: {list(df.columns)}")

    col_map = map_columns(df)
    print(f"  Mapeamento: {col_map}")

    # Valida mapeamento mínimo
    if "empresa" not in col_map or "ncm" not in col_map:
        print("  AVISO: mapeamento incompleto, tentando por posição...")
        cols = list(df.columns)
        # Detecta posição pelo conteúdo das células
        for ci, col in enumerate(cols):
            sample = df[col].dropna().astype(str).head(10).tolist()
            sample_str = " ".join(sample)
            # CNPJ tem padrão numérico longo
            if any(len(s.replace("/","").replace("-","").replace(".","")) >= 14 for s in sample):
                col_map.setdefault("cnpj", col)
            # NCM tem 8-9 dígitos
            if any(s.replace(".","").isdigit() and 7 <= len(s.replace(".","")) <= 9 for s in sample):
                col_map.setdefault("ncm", col)
            # Empresa tem textos longos
            if any(len(s) > 10 and s.replace(" ","").isalpha() for s in sample[:5]):
                col_map.setdefault("empresa", col)

    records = []
    skip_norms = {"NAN", "", "NONE", "IMPORTADOR", "RAZAO SOCIAL", "EMPRESA", "NOME"}

    for _, row in df.iterrows():
        emp = str(row.get(col_map.get("empresa", ""), "")).strip()
        if not emp or norm(emp) in skip_norms:
            continue

        ncm_raw = str(row.get(col_map.get("ncm", ""), "")).strip()
        ncm = ncm_raw.replace(".", "").replace(" ", "")
        if not ncm.isdigit() or len(ncm) < 6:
            continue

        try:
            kg_raw = str(row.get(col_map.get("kg", ""), 0))
            kg = float(kg_raw.replace(",", ".").replace(" ", ""))
        except:
            kg = 0
        if kg <= 0:
            continue

        pais_raw = str(row.get(col_map.get("pais", ""), "")).strip()
        pais = norm(pais_raw)  # sem acento para evitar duplicatas

        ua_raw = str(row.get(col_map.get("ua", ""), "")).strip()
        ua = norm(ua_raw)

        cnpj = str(row.get(col_map.get("cnpj", ""), "")).strip()
        mes = str(row.get(col_map.get("mes", ""), "")).strip()

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
