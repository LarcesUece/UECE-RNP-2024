import pandas as pd
import numpy as np
import re
import os 
from datetime import datetime, timedelta

def check_intervalo(timestamp1, timestamp2, timestamp3, timestamp4):
    time1 = datetime.fromtimestamp(timestamp1)
    time2 = datetime.fromtimestamp(timestamp2)
    time3 = datetime.fromtimestamp(timestamp3)
    time4 = datetime.fromtimestamp(timestamp4)
    time_diffs = [
        abs(time2 - time1),
        abs(time3 - time1),
        abs(time4 - time1),
        abs(time2 - time3),
        abs(time2 - time4),
        abs(time3 - time4)
    ]
    time_difference_ok = all(diff <= timedelta(hours=4) for diff in time_diffs)
    time1_maior = time1 > time2 and time1 > time3 and time1 > time4
    return time_difference_ok and time1_maior

def selecionar_traceroute(arquivo1, arquivo2, arquivo3, arquivo4, saida):
    df1 = pd.read_csv(arquivo1)  # Arquivo 1: Vazão
    df2 = pd.read_csv(arquivo2, header=None, sep=',', engine='python', on_bad_lines='skip')  # Arquivo 2: Traceroute
    df3 = pd.read_csv(arquivo3)  # Arquivo 3: Vazão
    df4 = pd.read_csv(arquivo4)  # Arquivo 4: Atraso

    linhas_encontradas = []

    for _, linha in df1.iterrows():
        try:
            timestamp1 = int(linha["Timestamp"])

            filtra_linha_df2 = df2[df2[0].apply(lambda x: check_intervalo(timestamp1, x))]
            if not filtra_linha_df2.empty:
                linha_prox_df2 = filtra_linha_df2.loc[filtra_linha_df2[0].idxmax()].copy()

                filtra_linha_df3 = df3[df3["Timestamp"].apply(lambda x: check_intervalo(timestamp1, x))]
                linha_prox_df3 = filtra_linha_df3.loc[filtra_linha_df3["Timestamp"].idxmax()] if not filtra_linha_df3.empty else None

                filtra_linha_df4 = df4[df4["Timestamp"].apply(lambda x: check_intervalo(timestamp1, x))]
                linha_prox_df4 = filtra_linha_df4.loc[filtra_linha_df4["Timestamp"].idxmax()] if not filtra_linha_df4.empty else None

                linha_prox_df2['Vazao_arquivo1'] = linha['Vazao']
                linha_prox_df2['Vazao_arquivo3'] = linha_prox_df3['Vazao'] if linha_prox_df3 is not None else None
                linha_prox_df2['Atraso_arquivo4'] = linha_prox_df4['Atraso(ms)'] if linha_prox_df4 is not None else None
                linha_prox_df2['Timestamp da vazao'] = linha['Timestamp']

                linhas_encontradas.append(linha_prox_df2)
        except (ValueError, TypeError, KeyError):
            continue

    result_df = pd.DataFrame(linhas_encontradas)
    if not result_df.empty:
        result_df.columns = df2.columns.append(pd.Index(['Vazao_arquivo1', 'Vazao_arquivo3', 'Atraso_arquivo4', 'Timestamp da vazao']))
        result_df.to_csv(saida, index=False)



if __name__ == "__main__":

    lista_arq_vazios = []
    local_vazaocubic = 'datasets/dataset 14-7/datasets vazao/cubic/'
    local_vazaobbr = 'datasets/dataset 14-7/datasets vazao/bbr/'
    local_traceroute = 'datasets/dataset 14-7/datasets traceroute/'
    local_atraso = 'datasets/dataset 14-7/datasets atraso/'
    local_destino = 'datasets/alterados/saida/'
    nome = "dataset-completo "

    if not os.path.exists(local_destino):
        os.makedirs(local_destino)

    # Carregar e ordenar os arquivos
    arquivo_1 = os.listdir(local_vazaocubic)
    arquivos1 = sorted(arquivo_1)

    arquivo_2 = os.listdir(local_traceroute)
    arquivos2 = sorted(arquivo_2)

    arquivo_3 = os.listdir(local_vazaobbr)
    arquivos3 = sorted(arquivo_3)

    arquivo_4 = os.listdir(local_atraso)
    arquivos4 = sorted(arquivo_4)

    # Loop pelos arquivos de vazão cubic
    for arquivo1 in arquivos1:
        print("file1 (Cubic):", arquivo1)
        nome_links1 = re.findall(r"\b\w{2}-\w{2}\b", arquivo1[:50])

        # Busca o arquivo de traceroute correspondente
        for arquivo2 in arquivos2:
            print("file 2 (Traceroute):", arquivo2)
            nome_links2 = re.findall(r"\b\w{2}-\w{2}\b", arquivo2[:40])
            if nome_links1[0] == nome_links2[0]:
                
                # Busca o arquivo de vazão BBR correspondente
                for arquivo3 in arquivos3:
                    nome_links3 = re.findall(r"\b\w{2}-\w{2}\b", arquivo3[:50])
                    if nome_links1[0] == nome_links3[0]:
                        
                        # Busca o arquivo de atraso correspondente
                        for arquivo4 in arquivos4:
                            nome_links4 = re.findall(r"\b\w{2}-\w{2}\b", arquivo4[:50])
                            if nome_links1[0] == nome_links4[0]:
                                print(f"Arquivos correspondentes encontrados: {arquivo1}, {arquivo2}, {arquivo3}, {arquivo4}")
                                
                                # Nome do arquivo de saída
                                saida = local_destino + nome + str(nome_links1[0]) + ".csv"
                                
                                # Chama a função ajustada para processar os 4 arquivos
                                selecionar_traceroute(
                                    local_vazaocubic + arquivo1,
                                    local_traceroute + arquivo2,
                                    local_vazaobbr + arquivo3,
                                    local_atraso + arquivo4,
                                    saida
                                )
                            else:
                                print("Arquivo de atraso não encontrado.")
                    else:
                        print("Arquivo de vazão BBR não encontrado.")
            else:
                print("Arquivo de traceroute não correspondente.")

    print("Arquivo com os resultados gerado com sucesso!")