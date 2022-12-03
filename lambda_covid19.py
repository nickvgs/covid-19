# %%
# Importando as bibliotecas
import pandas as pd 
import numpy as np
import glob
import psycopg2 as pg
# %%
# definindo local de arquivos
path = r'D:\Env\covid-19\data'
csv_files = glob.glob(path + "/*.csv")

# Definindo conexão com o banco de dados

conn = pg.connect(database="data-cov19",
                  host="localhost",
                  user="postgres",
                  password="admin",
                  port="5432"
                 )
cur = conn.cursor()
# %%
# Lendo os arquivos CSV e Craindo o DF

dfs = []
for files in csv_files:
    print(files)
    dfs.append(pd.read_csv(files,sep=';',skiprows=0))
    

df = pd.concat(dfs, ignore_index=True)

# %%

query = """INSERT INTO 
           data_covid
           (
                regiao
                estado
                municipio
                coduf
                codmun
                codregiaoSaude
                nomeregiaoSaude
                data
                semanaepi
                populacaotcu2019
                casosacumulado
                casosnovos
                obitosacumulado
                obitosnovos
                recuperadosnovos
                emacompanhamentonovos
                interiormetropolitana
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s,
                %s, %s
            ) 
        """
cur.execute(query, df)

# %%
regiao
estado
municipio
coduf
codmun
codRegiaoSaude
nomeRegiaoSaude
data
semanaEpi
populacaoTCU2019
casosAcumulado
casosNovos
obitosAcumulado
obitosNovos
Recuperadosnovos
emAcompanhamentoNovos
interioretropolitana