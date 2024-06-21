# %%
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from dateutil import parser
from datetime import datetime, timedelta
import requests
import openpyxl
from pandas_market_calendars import get_calendar
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, HTTPError, ConnectionError, Timeout
import sqlite3

# %% [markdown]
# # Funções

# %%
def workday() -> datetime:
    return get_calendar('BMF').schedule(
        data_ini=(datetime.now() - timedelta(days=90)).strftime("%d/%m/%Y"),
        data_fim=datetime.now().strftime("%d/%m/%Y")
    ).loc[::-1, ("market_open")].iloc[0].date()

# %% [markdown]
# # Main

# %%
USER = Path(os.path.expanduser("~"))

# %%
excel_path = USER / r'OneDrive - WISIR\General - WISIR\3 - OPERACIONAL\1 - DADOS\01 - PYTHON\02 - CONSOLIDADOS\COTAJ_ACAO_CONSOLIDADO.xlsx'
dados_cotacao__aj_consolidado_excel = pd.read_excel(excel_path)

# %%
dados_cotacao__aj_consolidado_excel

# %%
dados_cotacao__aj_consolidado_excel['DATA'] = pd.to_datetime(dados_cotacao__aj_consolidado_excel['DATA'], format="%d/%m/%Y")

# %%
ultima_linha_com_data = dados_cotacao__aj_consolidado_excel['DATA'].last_valid_index()

# %%
# CONSTRUIR O PAYLOAD COM AS DATAS DINÂMICAS
data_ini=(datetime.now() - timedelta(days=60)).strftime("%d/%m/%Y")
data_fim=datetime.now().strftime("%d/%m/%Y") 

url = "https://www.comdinheiro.com.br/Clientes/API/EndPoint001.php"
payload = (
    f"username=wisir.research&password=wisir.research&URL=HistoricoIndicadoresFundamentalistas001.php%3F"
    f"%26data_ini%3D{data_ini}%26data_fim%3D{data_fim}"
    "%26trailing%3D12%26conv%3DMIXED%26moeda%3DMOEDA_ORIGINAL%26c_c%3Dconsolidado%26m_m%3D1000000"
    "%26n_c%3D2%26f_v%3D0%26papel%3Dexplode%28STOCK_GUIDE_WISIR%29"
    "%26indic%3DPRECO_AJ%28%2C%2C%2CA%2CC%29%26periodicidade%3Ddu%26graf_tab%3Dtabela%26desloc_data_analise%3D1"
    "%26flag_transpor%3D0%26c_d%3Dc%26enviar_email%3D0%26enviar_email_log%3D0%26cabecalho_excel%3Dmodo1"
    "%26relat_alias_automatico%3Dcmd_alias_01&format=json3"
)
        
headers = {"Content-Type": "application/x-www-form-urlencoded"}
querystring = {"code": "import_data"}
        
#SOLICITAÇÃO DA API
response = requests.post(url, data=payload, headers=headers, params=querystring, timeout=1800)

# %%
novos_dados = response.json()

# %%
data = list()
for lin in (table := response.json()["tables"]["tab0"]):
    data.append(table[lin])

# %%
dados_cotacao__aj_novos = pd.DataFrame(data)

# %%
dados_cotacao__aj_novos = dados_cotacao__aj_novos.rename(
    columns = {
        # Dict comprehension para colocar os nomes antigos da coluna com os valores da primeira linha.
        nome_antigo_da_coluna: nome_novo_da_coluna for
            nome_antigo_da_coluna, nome_novo_da_coluna in
                # Zip para poder fazer unpack das duas informacoes
                zip(
                    list(dados_cotacao__aj_novos.columns), [
                        # Como os dados da primeira linha vem em formatod indeseja ja formatamos aqui
                        str(item).replace("\nPRECO_AJ(,,,A,C)", "") for
                            item in
                                # iloc para pegar a primeira linha e todas as colunas
                                list(dados_cotacao__aj_novos.iloc[0, :])
                    ]
                )
        }
)

# %%
dados_cotacao__aj_novos = dados_cotacao__aj_novos.drop(index = 0)

# %%
# ARRUMA AS DATAS ERRADAS PARA O FORMATO DAS OUTRAS
parser.parse("12-06-2024", dayfirst = True)

# %%
dados_cotacao__aj_novos["Data"] = dados_cotacao__aj_novos["Data"].map(lambda row: parser.parse(row, dayfirst = True))

# %%
dados_cotacao__aj_novos = dados_cotacao__aj_novos.rename(columns = {"Data": "DATA"}).astype(dtype = {"DATA": "datetime64[ns]"})

# %%
dados_cotacao__aj_novos.reset_index(drop = True)

# %%
if dados_cotacao__aj_consolidado_excel.index.duplicated().any():
    dados_cotacao__aj_consolidado_excel = dados_cotacao__aj_consolidado_excel.reset_index(drop=True)
if dados_cotacao__aj_novos.index.duplicated().any():
    dados_cotacao__aj_novos = dados_cotacao__aj_novos.reset_index(drop=True)

# %%
dados_cotacao__aj_consolidado_excel = dados_cotacao__aj_consolidado_excel.loc[:, ~dados_cotacao__aj_consolidado_excel.columns.duplicated()]
dados_cotacao__aj_novos = dados_cotacao__aj_novos.loc[:, ~dados_cotacao__aj_novos.columns.duplicated()]

# %%
# CONCATENA OS DADOS 
dados_cotacao_aj_combinado = pd.concat([dados_cotacao__aj_consolidado_excel, dados_cotacao__aj_novos], ignore_index=True)
dados_cotacao_aj_combinado = dados_cotacao_aj_combinado.drop_duplicates(subset=['DATA']).reset_index(drop=True)

# %%
dados_cotacao_aj_combinado

# %%
# SALVA O DF CONCATENANDO NO EXCEL
output_path = USER / r'OneDrive - WISIR\General - WISIR\3 - OPERACIONAL\1 - DADOS\01 - PYTHON\02 - CONSOLIDADOS\COTAJ_ACAO_CONSOLIDADO.xlsx'

dados_cotacao_aj_combinado = dados_cotacao_aj_combinado.drop(dados_cotacao_aj_combinado.index[-1])
dados_cotacao_aj_combinado.to_excel(output_path, index=False)

df = pd.read_excel(output_path)
df.replace(['-', '[]'], np.nan, inplace=True)
df = df.dropna(how='all', subset=df.columns.difference(['DATA']))
df.to_excel(output_path, index=False)


print('Dados atualizados e salvos no Excel.')
print('URL construída:', url + payload)


