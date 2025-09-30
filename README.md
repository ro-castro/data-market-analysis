📊 Market Insights Brazil

Solução completa para pesquisa de mercado e inteligência de negócios baseada em dados públicos de empresas brasileiras.
O projeto combina Python (pandas), SQL e Power BI, permitindo identificar e analisar empresas de acordo com seus CNAEs (Classificação Nacional de Atividades Econômicas), situação cadastral, porte e localização.

🚀 Objetivo

Criar uma solução para explorar e segmentar empresas no Brasil com base nos dados oficiais do governo.

Possibilitar análises por setor, porte, localização (CEP/UF/município) e ano de início de atividade.

Apoiar pesquisas de mercado, identificação de clientes potenciais e estudos de inteligência competitiva.

🛠️ Estrutura do Projeto

Python (pandas)

Filtros iniciais de empresas por CNAE.

Seleção de empresas ativas pela situação cadastral.

Geração de arquivos reduzidos para carga em SQL/Power BI.

SQL

Limpeza e padronização da base (~64M registros).

Correção de inconsistências (porte da empresa, datas, campos nulos, etc.).

Criação de views otimizadas para consulta e conexão com Power BI.

Power BI

Página 1: Mapa interativo por CEP/UF com filtros e tabela detalhada.

Página 2: Distribuição de empresas por setores (CNAE).

Futuras páginas: análise por porte, ano de início de atividade, região vs setor.
