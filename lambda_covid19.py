# %%
# Importando as bibliotecas
import pandas as pd 
import numpy as np
import glob
import psycopg2 as pg
from typing import NoReturn

path = r'D:\Env\covid-19\data'
csv_files = glob.glob(path + "/*.csv")

# Definindo conexão com o banco de dados

conn = pg.connect(
                host="localhost",
                database="data-cov19_2",
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
# sep informa qual o separador no arquivo 
# skiprows pula a leitura da primeira linha (pois contem o cabeçalho de cada arquivo)

df = pd.concat(dfs, ignore_index=True)

# %% 
# Renomeando DF para transformar em iteraveis

df_x = df.rename(columns={
        'regiao':'regiao',
        'estado':'estado',
        'municipio':'municipio',
        'coduf':'coduf',
        'codmun':'codmun',
        'codRegiaoSaude':'codregiaosaude',
        'nomeRegiaoSaude':'nomeregiaosaude',
        'data':'data',
        'semanaEpi':'semanaepi',
        'populacaoTCU2019':'populacaotcu2019',
        'casosAcumulado':'casosacumulado',
        'casosNovos':'casosnovos',
        'obitosAcumulado':'obitosacumulado',
        'obitosNovos':'obitosnovos',
        'Recuperadosnovos':'recuperadosnovos',
        'emAcompanhamentoNovos':'emacompanhamentonovos',
        'interior/metropolitana':'interiormetropolitana'            
})
    
    

# %%
# Transformando os dados em iteráveis para o push
rows = zip(
    df_x.regiao,
    df_x.estado,
    df_x.municipio,
    df_x.coduf,
    df_x.codmun,
    df_x.codregiaosaude,
    df_x.nomeregiaosaude,
    df_x.data,
    df_x.semanaepi,
    df_x.populacaotcu2019,
    df_x.casosacumulado,
    df_x.casosnovos,
    df_x.obitosacumulado,
    df_x.obitosnovos,
    df_x.recuperadosnovos,
    df_x.emacompanhamentonovos,
    df_x.interiormetropolitana
)

# %%
# Query para a inserção de dados em SQL
query = """INSERT INTO data_covid
        (
            regiao,
            estado,
            municipio,
            coduf,
            codmun,
            codregiaosaude,
            nomeregiaosaude,
            data,
            semanaepi,
            populacaotcu2019,
            casosacumulado,
            casosnovos,
            obitosacumulado,
            obitosnovos,
            recuperadosnovos,
            emacompanhamentonovos,
            interiormetropolitana
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s,
                %s, %s
            ) 
        """


# %%
# Insert dos dados na tabela 
cur.executemany(query, rows)
conn.commit()
cur.close()


