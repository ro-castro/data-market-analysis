# 📊 Market Insights Brazil

Solução completa para **pesquisa de mercado e inteligência de negócios** baseada em **dados públicos de empresas brasileiras (Receita Federal)**. O projeto integra **Python (pandas)**, **SQL** e **Power BI** para filtrar, tratar e analisar milhões de registros, permitindo identificar empresas por **CNAE**, **situação cadastral**, **porte**, **localização** e **tempo de atividade**.

> ⚠️ **Aviso**: este projeto utiliza exclusivamente dados públicos. Não há enriquecimento com dados sensíveis ou privados.

---

## 🚀 Objetivos

* Explorar e segmentar empresas no Brasil a partir de dados oficiais.
* Identificar oportunidades de mercado e clientes potenciais por **setor (CNAE)**.
* Permitir análises por **porte**, **localização (CEP/UF/Município)** e **ano de início de atividade**.
* Apoiar **pesquisas de mercado**, **planejamento comercial** e **inteligência competitiva**.

---

## 🧠 Visão Geral da Arquitetura

```text
Dados Públicos (RFB)
        ↓
Python (pandas)
- Leitura em chunks (~64M registros)
- Filtro por CNAE e situação cadastral
- Normalização de campos
        ↓
SQL
- Limpeza e padronização
- Views otimizadas
        ↓
Power BI
- Dashboards interativos
- Análises geográficas e setoriais
```

---

## 🛠️ Tecnologias Utilizadas

* **Python 3.x**

  * pandas
  * pathlib
* **SQL** (PostgreSQL / SQL Server / MySQL – adaptável)
* **Power BI**
* **Dados Abertos da Receita Federal (CNPJ)**

---

## 📂 Estrutura do Projeto

```text
data-market-analysis/
│
├── .git/                    # Metadados internos do Git (controle de versão)
│
├── dados_brutos/             # Dados brutos da Receita Federal
│   ├── dados_cnpj/           # Estabelecimentos (arquivos CSV originais)
│   └── dados_empresas/       # Empresas (arquivos CSV originais)
│
├── dados_filtrados/          # Dados processados e prontos para análise
│   ├── cnpj_filtrado.csv     # Estabelecimentos filtrados (ativos + CNAE)
│   └── empresas_filtradas.csv# Dados consolidados das empresas
│
├── python/                   # Scripts de processamento e pipeline
│   └── pesquisamercado2_opcao2_final.py
│
├── sql/                      # Scripts SQL (views, limpeza, modelagem)
│
├── pbi/                      # Arquivos do Power BI (.pbix)
│
└── README.md                 # Documentação do projeto
```

---

## 🐍 Pipeline em Python (pandas)

Responsável pelo **pré-processamento pesado** dos dados:

* Leitura de arquivos CSV em **chunks** para suportar bases com dezenas de milhões de registros.
* Filtro de empresas:

  * CNAEs de interesse (lista configurável).
  * Situação cadastral **ativa**.
* Normalização de campos:

  * CNPJ
  * CNAE principal
  * Datas
* Geração de arquivos reduzidos para carga eficiente em SQL ou Power BI.

### Saídas geradas

* `clientes_filtrados.csv`
* `empresas_filtradas.csv`

---

## 🗄️ Camada SQL

Utilizada para **tratamento final e performance analítica**:

* Limpeza e padronização da base consolidada.
* Correção de inconsistências:

  * Porte da empresa
  * Datas inválidas ou nulas
  * Campos inconsistentes
* Criação de **views otimizadas** para consumo no Power BI.

---

## 📊 Power BI – Dashboards

Dashboards interativos voltados para análise exploratória e tomada de decisão.

### Páginas atuais

* **Mapa Geográfico**

  * Distribuição de empresas por CEP / UF / Município
  * Filtros por CNAE e porte
  * Tabela detalhada de empresas

* **Análise Setorial (CNAE)**

  * Distribuição por setor
  * Comparação entre regiões

### Próximas evoluções

* Análise por **porte da empresa**
* Empresas por **ano de início de atividade**
* Cruzamento **região × setor**
* Indicadores de densidade de mercado

---

## ▶️ Como Executar

1. Baixe os dados públicos da Receita Federal (CNPJ).
2. Organize os arquivos:

```text
dados_cnpj/       → Estabelecimentos
dados_empresas/   → Empresas
```

3. Execute o pipeline:

```bash
python pesquisamercado2.py
```

4. Importe os CSVs gerados em seu banco SQL ou diretamente no Power BI.

---

## 📈 Casos de Uso

* Prospecção B2B e geração de leads
* Estudos de viabilidade de mercado
* Planejamento de expansão regional
* Análise competitiva por setor

---

## 🤝 Contribuições

Contribuições são bem-vindas!

* Fork o projeto
* Crie uma branch (`feature/minha-feature`)
* Abra um Pull Request

---

## ✍️ Autor

**Rodrigo Castro**
Consultor | Dados | Inteligência de Mercado
