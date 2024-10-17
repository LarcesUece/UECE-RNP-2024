import os
import re
from datetime import datetime, timedelta

import pandas as pd

def check_intervalo(timestamp1, timestamp2):
    time1 = datetime.fromtimestamp(timestamp1)
    time2 = datetime.fromtimestamp(timestamp2)
    time_difference = time2 - time1
    return abs(time_difference) <= timedelta(minutes=10) and (time1 > time2)

def selecionar_traceroute(arquivo1, arquivo2, saida):
    df1 = pd.read_csv(arquivo1)
    df2 = pd.read_csv(arquivo2, header=None, sep=',', engine='python', on_bad_lines='skip')
    linhas_encontradas = []

    for _, linha in df1.iterrows():
        try:
            # Verifica se o valor é um número e converte para inteiro
            timestamp1 = int(linha["Timestamp"])

            # Filtra os dados de df2 com base no intervalo de tempo
            filtra_linha = df2[df2[0].apply(lambda x: check_intervalo(timestamp1, x))]

            if not filtra_linha.empty:
                # Seleciona a linha com o maior timestamp próximo ao timestamp1
                linha_prox = filtra_linha.loc[filtra_linha[0].idxmax()].copy()  
                
                # Adiciona a coluna 'Vazao' correspondente
                linha_prox['Vazao'] = linha['Vazao']

                # Adiciona a linha com a coluna 'Vazao' à lista de linhas encontradas
                linhas_encontradas.append(linha_prox)
        except (ValueError, TypeError, KeyError):
            # Tratar erros de conversão ou ausência de chave "Timestamp"
            continue

    # Salva os resultados no arquivo de saída
    result_df = pd.DataFrame(linhas_encontradas)
    if not result_df.empty:
        result_df.columns = df2.columns.append(pd.Index(['Vazao']))  # Adiciona 'Vazao' como nova coluna
        result_df.to_csv(saida, index=False)
    else:
        lista_arq_vazios.append(arquivo1)

if __name__ == "__main__":

    lista_arq_vazios = []
    local_origem1 = 'datasets/dataset 14-7/datasets vazao/cubic/'
    #local_origem1 = "datasets vazao\\cubic\\"
    local_origem2 = "datasets/dataset 14-7/datasets traceroute/"
    local_destino = "datasets/alterados/saida/cubic/"
    #local_destino = "datasets alterados\\saida\\cubic\\"
    nome = "traceroute da vazao "

    if not os.path.exists(local_destino):
        os.makedirs(local_destino)

    arquivo_1 = os.listdir(local_origem1)
    arquivos1 = sorted(arquivo_1)

    arquivo_2 = os.listdir(local_origem2)
    arquivos2 = sorted(arquivo_2)

    for arquivo1 in arquivos1:
        print("file1:", arquivo1)
        nome_links1 = re.findall(r"\b\w{2}-\w{2}\b", arquivo1[:50])
        for arquivo2 in arquivos2:
            print("file 2:", arquivo2)
            nome_links2 = re.findall(r"\b\w{2}-\w{2}\b", arquivo2[:40])
            if nome_links1[0] == nome_links2[0]:
                print(arquivo1)
                print(arquivo2)
                saida = local_destino + nome + str(nome_links1[0]) + ".csv"
                selecionar_traceroute(
                    local_origem1 + arquivo1, local_origem2 + arquivo2, saida
                )
            else:
                print("buscando o arquivo correto")

    print("Arquivo com os resultados gerado com sucesso!")
 