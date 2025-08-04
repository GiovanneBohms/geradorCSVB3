import pandas as pd
import numpy as np
import os
import time

tempo1 = time.time()

pasta_saida = r".\AcoesCSV"
os.makedirs(pasta_saida, exist_ok=True)

arquivos_entrada = [
    r".\COTAHIST_A2022.TXT",
    r".\COTAHIST_A2023.TXT",
    r".\COTAHIST_A2024.TXT",
    r".\COTAHIST_A2025.TXT"
]

linhas_totais = []
for arquivo in arquivos_entrada:
    with open(arquivo, 'r') as f:
        linhas = f.readlines()[1:-1]
    linhas_totais.extend(linhas)

print(f"Total de linhas lidas: {len(linhas_totais)}")

dic = {
    'DATA DO PREGÃO': pd.to_datetime([linha[2:10] for linha in linhas_totais]),
    'CÓDIGO DE NEGOCIAÇÃO DO PAPEL': [linha[12:24].strip() for linha in linhas_totais],
    'TIPO DE MERCADO': [int(linha[24:27].strip()) for linha in linhas_totais],
    'NOME RESUMIDO': [linha[27:39].strip() for linha in linhas_totais],
    'ESPECIFICAÇÃO DO PAPEL': [linha[39:49].strip('-') for linha in linhas_totais],
    'ABERTURA': [float(linha[56:69])/100 for linha in linhas_totais],
    'MÁXIMO': [float(linha[69:82])/100 for linha in linhas_totais],
    'MÍNIMO': [float(linha[82:95])/100 for linha in linhas_totais],
    'FECHAMENTO': [float(linha[108:121])/100 for linha in linhas_totais],
    'VOLUME': [float(linha[170:188])/100 for linha in linhas_totais]
}

dtype_dict = {
    'CÓDIGO DE NEGOCIAÇÃO DO PAPEL': 'category',
    'TIPO DE MERCADO': np.int8,
    'NOME RESUMIDO': 'category',
    'ESPECIFICAÇÃO DO PAPEL': 'category',
    'ABERTURA': np.float32,
    'MÁXIMO': np.float32,
    'MÍNIMO': np.float32,
    'FECHAMENTO': np.float32,
    'VOLUME': np.float64
}

df = pd.DataFrame(dic).astype(dtype_dict)

tipos_validos = [10, 11, 17, 33]
df = df[df['TIPO DE MERCADO'].isin(tipos_validos)]

print(f"Total de linhas após filtro de tipo de mercado: {len(df)}")

volume_mediano_minimo = 100000
dias_minimos_negociados = 200

ativos_validos = []
ativos_excluir = []

# Identifica os 3 dias úteis mais recentes no dataset
ultimos_3_dias = sorted(df['DATA DO PREGÃO'].unique())[-3:]

for codigo, grupo in df.groupby('CÓDIGO DE NEGOCIAÇÃO DO PAPEL', observed=True):
    mediana_volume = grupo['VOLUME'].median()
    dias_com_volume = (grupo['VOLUME'] > 0).sum()
    volume_ultimos_dias = grupo[grupo['DATA DO PREGÃO'].isin(ultimos_3_dias)]['VOLUME']
    teve_volume_recente = (volume_ultimos_dias > 0).any()

    cond_mediana = mediana_volume >= volume_mediano_minimo
    cond_dias_negociados = dias_com_volume >= dias_minimos_negociados
    cond_volume_recente = teve_volume_recente

    if cond_mediana and cond_dias_negociados and cond_volume_recente:
        ativos_validos.append(codigo)
    else:
        ativos_excluir.append(codigo)
        if codigo == 'PETR4':
            print(f"\nPETR4 NÃO passou no filtro:")
            if not cond_mediana:
                print(f" - Mediana do volume diário é {mediana_volume:.0f}, abaixo do mínimo {volume_mediano_minimo}")
            if not cond_dias_negociados:
                print(f" - Possui apenas {dias_com_volume} dias com volume > 0 (mínimo exigido: {dias_minimos_negociados})")
            if not cond_volume_recente:
                print(f" - Não teve volume nos últimos 3 dias úteis: {ultimos_3_dias}")

print(f"\nTotal de ativos que passaram no filtro: {len(ativos_validos)}")
print(f"Total de ativos que NÃO passaram no filtro: {len(ativos_excluir)}")


df_validos = df[df['CÓDIGO DE NEGOCIAÇÃO DO PAPEL'].isin(ativos_validos)]

total = len(ativos_validos)
print(f"\nIniciando salvamento de {total} arquivos CSV...")

for i, (codigo, df_papel) in enumerate(df_validos.groupby('CÓDIGO DE NEGOCIAÇÃO DO PAPEL', observed=True), start=1):
    df_papel = df_papel.sort_values('DATA DO PREGÃO')
    nome_arquivo = f"{codigo}.csv"
    caminho_arquivo = os.path.join(pasta_saida, nome_arquivo)
    df_papel.to_csv(caminho_arquivo, index=False)

    if i % max(1, total // 20) == 0 or i == total:
        perc = (i / total) * 100
        print(f"Progresso: {i}/{total} arquivos salvos ({perc:.1f}%)")

tempo2 = time.time()
print(f"\nProcesso concluído em {round(tempo2 - tempo1, 2)} segundos.")
