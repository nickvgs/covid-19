# %%
# Importando as bibliotecas
import pandas as pd 
import numpy as np
import glob

# %%
# definindo local de arquivis
path = r'D:\Env\covid-19\data'
csv_files = glob.glob(path + "/*.csv")


# %%
# Lendo os arquivos CSV e Craindo o DF

dfs = []
for files in csv_files:
    print(files)
    dfs.append(pd.read_csv(files,sep=';',skiprows=0))
    

df = pd.concat(dfs, ignore_index=True)

# %%

# %%
