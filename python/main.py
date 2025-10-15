# pesquisamercado2_opcao2_final.py
import pandas as pd
import os
import sys
from pathlib import Path

# ===== Pastas =====
BASE_DIR = Path(__file__).resolve().parent
PASTA_CNPJ = BASE_DIR / "dados_cnpj"         # Estabelecimentos
PASTA_EMPRESAS = BASE_DIR / "dados_empresas" # Empresas
PASTA_SAIDA = BASE_DIR / "dados_filtrados"
PASTA_SAIDA.mkdir(exist_ok=True)

OUTPUT_CLIENTES = PASTA_SAIDA / "clientes_filtrados.csv"
OUTPUT_EMPRESAS = PASTA_SAIDA / "empresas_filtradas.csv"

# ===== Lista de CNAEs de interesse =====
CNAES_LISTA = [
    "1061902","0113000","0131800","0133404","0134200","0141501","0141502","0500301",
    "0600002","0600003","0710301","0721901","0722701","0723501","0724301","0725100",
    "0729401","0729402","0729403","0729404","0810001","0810002","0810003","0810004",
    "0810005","0810006","0810007","0810008","0810009","0810010","0810099","0891600",
    "0893200","0899101","0899102","0899103","0899199","1041400","1042200","1043100",
    "1064300","1065102","1065103","1066000","1071600","1072401","1072402","1081301",
    "1081302","1082100","1091100","1091101","1093701","1096100","1099607","1311100",
    "1312000","1622699","1922599","2012600","2013400","2013401","2013402","2019399",
    "2021500","2022300","2029100","2031200","2032100","2051700","2061400","2099199",
    "2221800","2222600","2223400","2229302","2229303","2229399","2320600","2330301",
    "2330302","2330303","2330305","2330399","2342702","2391501","3832700","3839401",
    "4621400","4623106","4623108","4623109","4623199","4637101","4637102","4637107",
    "4674500","4679604","4679699","4683400","4684201","4684299","4685100","4689301",
    "1061901","1062700","1063500","1069400","1041700","1042501","1042502","1079900",
    "1092900","1093702"
]

# ===== Colunas desejadas =====
COLUNAS_ESTAB = [
    "CNPJ_BASICO","CNPJ_ORDEM","CNPJ_DV",
    "IDENTIFICADOR_MATRIZ_FILIAL","NOME_FANTASIA","SITUACAO_CADASTRAL",
    "DATA_SITUACAO_CADASTRAL","MOTIVO_SITUACAO_CADASTRAL",
    "NOME_CIDADE_EXTERIOR","PAIS","DATA_INICIO_ATIVIDADE",
    "CNAE_FISCAL_PRINCIPAL","CNAE_FISCAL_SECUNDARIA","TIPO_LOGRADOURO",
    "LOGRADOURO","NUMERO","COMPLEMENTO","BAIRRO","CEP","UF","MUNICIPIO",
    "DDD1","TELEFONE1","DDD2","TELEFONE2","DDD_FAX","FAX",
    "CORREIO_ELETRONICO","SITUACAO_ESPECIAL","DATA_SITUACAO_ESPECIAL"
]

COLUNAS_EMPRESAS = [
    "CNPJ_BASICO","RAZAO_SOCIAL","NATUREZA_JURIDICA",
    "QUALIFICACAO_DO_RESPONSAVEL","CAPITAL_SOCIAL","PORTE","ENTE_FEDERATIVO"
]

# ===== Mapas de índice (0-based) — confira com um arquivo de amostra =====
# Estabelecimentos (layout RFB comum)
ESTAB_IDX = {
    "CNPJ_BASICO": 0,
    "CNPJ_ORDEM": 1,
    "CNPJ_DV": 2,
    "IDENTIFICADOR_MATRIZ_FILIAL": 3,
    "NOME_FANTASIA": 4,
    "SITUACAO_CADASTRAL": 5,
    "DATA_SITUACAO_CADASTRAL": 6,
    "MOTIVO_SITUACAO_CADASTRAL": 7,
    "NOME_CIDADE_EXTERIOR": 8,
    "PAIS": 9,
    "DATA_INICIO_ATIVIDADE": 10,
    "CNAE_FISCAL_PRINCIPAL": 11,     # ATENÇÃO: geralmente é a 12ª coluna (0-based 11)
    "CNAE_FISCAL_SECUNDARIA": 12,
    "TIPO_LOGRADOURO": 13,
    "LOGRADOURO": 14,
    "NUMERO": 15,
    "COMPLEMENTO": 16,
    "BAIRRO": 17,
    "CEP": 18,
    "UF": 19,
    "MUNICIPIO": 20,
    "DDD1": 21,
    "TELEFONE1": 22,
    "DDD2": 23,
    "TELEFONE2": 24,
    "DDD_FAX": 25,
    "FAX": 26,
    "CORREIO_ELETRONICO": 27,
    "SITUACAO_ESPECIAL": 28,
    "DATA_SITUACAO_ESPECIAL": 29,
}

# Empresas (layout RFB comum)
EMP_IDX = {
    "CNPJ_BASICO": 0,
    "RAZAO_SOCIAL": 1,                 # nome empresarial
    "NATUREZA_JURIDICA": 2,
    "QUALIFICACAO_DO_RESPONSAVEL": 3,
    "CAPITAL_SOCIAL": 4,
    "PORTE": 5,
    "ENTE_FEDERATIVO": 6,
}

# ===== usecols =====
ESTAB_USECOLS = [ESTAB_IDX[c] for c in COLUNAS_ESTAB if c in ESTAB_IDX]
EMP_USECOLS   = [EMP_IDX[c]   for c in COLUNAS_EMPRESAS if c in EMP_IDX]

def processar_csv_estab(csv_path: Path) -> pd.DataFrame:
    print(f"Processando estabelecimentos: {csv_path.name}")
    chunk_iter = pd.read_csv(
        csv_path, sep=";", header=None, chunksize=100_000,
        dtype=str, encoding="latin1", low_memory=False, usecols=ESTAB_USECOLS
    )

    out_chunks = []
    for chunk in chunk_iter:
        # Renomeia colunas numéricas -> nomes finais
        chunk = chunk.rename(columns={ESTAB_IDX[k]: k for k in ESTAB_IDX if ESTAB_IDX[k] in chunk.columns})

        # Garante todas as colunas pedidas (se faltar alguma no arquivo)
        for col in COLUNAS_ESTAB:
            if col not in chunk.columns:
                chunk[col] = ""

        # Normalizações mínimas
        chunk["CNPJ_BASICO"] = chunk["CNPJ_BASICO"].fillna("").str.strip().str.zfill(8)
        chunk["SITUACAO_CADASTRAL"] = chunk["SITUACAO_CADASTRAL"].fillna("").str.strip().str.zfill(2)

        # CNAE principal: LIMPA E SOBRESCREVE na própria coluna (sem criar outra)
        chunk["CNAE_FISCAL_PRINCIPAL"] = (
            chunk["CNAE_FISCAL_PRINCIPAL"].fillna("")
            .astype(str).str.split(",").str[0].str.strip()         # se vier "1234567,..." pega o primeiro
            .str.replace(r"\D", "", regex=True).str.zfill(7)       # só dígitos, 7 chars
        )

        # Filtro: ativos + CNAE de interesse
        m = (chunk["SITUACAO_CADASTRAL"] == "02") & (chunk["CNAE_FISCAL_PRINCIPAL"].isin(CNAES_LISTA))
        if m.any():
            out_chunks.append(chunk.loc[m, COLUNAS_ESTAB].copy())

    if not out_chunks:
        return pd.DataFrame(columns=COLUNAS_ESTAB)

    df = pd.concat(out_chunks, ignore_index=True)
    # Evita duplicatas (mesmo CNPJ_BASICO e CNAE principal)
    df.drop_duplicates(subset=["CNPJ_BASICO","CNAE_FISCAL_PRINCIPAL"], inplace=True)
    return df

def processar_csv_empresa(csv_path: Path, cnpjs_validos: set) -> pd.DataFrame:
    print(f"Processando empresas: {csv_path.name}")
    chunk_iter = pd.read_csv(
        csv_path, sep=";", header=None, chunksize=100_000,
        dtype=str, encoding="latin1", low_memory=False, usecols=EMP_USECOLS
    )

    out_chunks = []
    for chunk in chunk_iter:
        chunk = chunk.rename(columns={EMP_IDX[k]: k for k in EMP_IDX if EMP_IDX[k] in chunk.columns})

        for col in COLUNAS_EMPRESAS:
            if col not in chunk.columns:
                chunk[col] = ""

        chunk["CNPJ_BASICO"] = chunk["CNPJ_BASICO"].fillna("").str.strip().str.zfill(8)

        m = chunk["CNPJ_BASICO"].isin(cnpjs_validos)
        if m.any():
            out_chunks.append(chunk.loc[m, COLUNAS_EMPRESAS].copy())

    if not out_chunks:
        return pd.DataFrame(columns=COLUNAS_EMPRESAS)

    df = pd.concat(out_chunks, ignore_index=True)
    df.drop_duplicates(subset=["CNPJ_BASICO"], inplace=True)
    return df

# ===== Pipeline =====
arquivos_estab = sorted(PASTA_CNPJ.glob("*.csv"))
if not arquivos_estab:
    print(f"Nenhum CSV encontrado em {PASTA_CNPJ}")
    sys.exit(1)

dfs_clientes = []
for p in arquivos_estab:
    df = processar_csv_estab(p)
    if not df.empty:
        dfs_clientes.append(df)

df_clientes = pd.concat(dfs_clientes, ignore_index=True) if dfs_clientes else pd.DataFrame(columns=COLUNAS_ESTAB)
df_clientes.to_csv(OUTPUT_CLIENTES, index=False, sep=";")
print(f"\n✅ Parte 1 concluída. Registros: {len(df_clientes):,} | Arquivo: {OUTPUT_CLIENTES}")

cnpjs_validos = set(df_clientes["CNPJ_BASICO"].unique())
print(f"CNPJs base únicos: {len(cnpjs_validos):,}")

arquivos_emp = sorted(PASTA_EMPRESAS.glob("*.csv"))
if not arquivos_emp:
    print(f"Nenhum CSV encontrado em {PASTA_EMPRESAS}")
    sys.exit(1)

dfs_empresas = []
for p in arquivos_emp:
    df = processar_csv_empresa(p, cnpjs_validos)
    if not df.empty:
        dfs_empresas.append(df)

df_empresas = pd.concat(dfs_empresas, ignore_index=True) if dfs_empresas else pd.DataFrame(columns=COLUNAS_EMPRESAS)
df_empresas.to_csv(OUTPUT_EMPRESAS, index=False, sep=";")
print(f"\n✅ Parte 2 concluída. Registros: {len(df_empresas):,} | Arquivo: {OUTPUT_EMPRESAS}")
