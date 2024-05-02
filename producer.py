import requests
import csv
import time

stack = []
lock = False
end_script = False

## Função que busca os CNAES 
def fetch_cnaes(cnpj):
    # Url padrão, concatenada com os CNPJ's desejados
    url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj}"
    # Resposta da API
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Acesso ao campo "Atividade principal", para pegar o CNAE
        atividade_principal = data.get("atividade_principal", [])
        if atividade_principal:
            # Retornando o CNAE de determinado CNPJ
            return atividade_principal[0].get("code", "")
    return None

## Função principal do productor
def main():
    # Variáveis auxiliares globais
    global lock
    global end_script
    
    # Contador para saber o número de requisições (API só permite 3 por minuto)
    counter = 0
    with open("source/cnpjs.txt", "r") as file:
        cnpjs = [line.strip() for line in file]

    with open("cnpjs_cnaes.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(["CNPJ", "CNAE"])
        # Pegamos o último CNPJ
        last_cnpj = cnpjs[-1]
        # Para cada CNPJ, pegamos o CNAE
        for cnpj in cnpjs:
            cnae = fetch_cnaes(cnpj)
            # Quando acha um CNPJ, escreve no arquivo CSV (CNJP;CNAE)
            if cnae:
                writer.writerow([cnpj, cnae])
                # Variável utilizada para controlar as duas threads (evitar algum problema na lista "stack")
                lock = True
                # Adiciona os dados que foram salvos em uma lista chamada "stack", 
                # lista utilizada para o consumidor acessar os dados sem precisar abrir arquivo
                stack.append([cnpj, cnae])
            counter += 1
            # Conta 3 requisições para a API, tendo em vista que o limite da mesma é 3, ou verifica se o CNPJ atual é o último.
            if counter == 3 or last_cnpj == cnpj:
                counter = 0
                # Libera a lista "stack" para ser utilizada pelo consumidor
                lock = False
                # Dorme a thread por 65 segundos, tempo necessário para uma nova requisição para a API
                time.sleep(65)
        # Variável que finaliza a outra thread e fechamento de arquivos.
        csvfile.close()
        file.close()
        end_script = True
        print("Produtor terminou")
        
                
    
