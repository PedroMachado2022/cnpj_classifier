import producer
import time

stack = []
end_consumer = False
def classify_cnpjs(cnaes):
    types = {
        "4771701": "Farmácias",
        "4644301": "Farmácias",
        "4771703": "Farmácias",
        "4771702": "Farmácias",
        "9321200": "Parques",
        "9329899": "Parques",
        "8610101": "Hospitais",
        "8610102": "Hospitais",
        "8532500": "Hospitais",
        "8412400": "Hospitais",
        "4773300": "Ortopedias",
        "4664800": "Ortopedias",
        "4645101": "Ortopedias",
        "7729203": "Ortopedias",
        "4645102": "Ortopedias",
        "4645103": "Ortopedias",
        "3250703": "Ortopedias",
        "4754702": "Ortopedias",
    }

    # Filtramos o código CNAE que vem com caracteres adicionais, para um código composto apenas por números
    cnae_numerico = ''.join(filter(str.isdigit, cnaes[1]))
    
    # Selecionamos o tipo do CNPJ, comparando o CNAE do mesmo com o dicionário de CNAE's "types"
    tipo = types.get(str(cnae_numerico), "Outros")   
    
    #print(type(arquivo_entrada[0]))
    with open(f"cnpj_{tipo}.txt", "a+") as outfile:
        outfile.write(f"{cnaes[0]}\n")
    outfile.close()

# Função que recebe os dados do produtor
def consumer_function():
    global stack
    # Verificação para saber se a lista "stack" está liberada e se a lista de CNPJ/CNAE não está vazia 
    if (not(producer.lock) and producer.stack != []):
        # Para cada [CNPJ, CNAE], chama a função de classificação, passando essa sublista como argumento
        for cnae in producer.stack:
            classify_cnpjs(cnae)
        
        # Limpa a lista após fazer a operação
        producer.stack = []
        #print(f"Producer Stack {producer.stack}")
        #print(f"Stack {stack}")

# Função de controle do consumidor
def main():
    global end_consumer
    # Enquanto a lista de CNPJ's não finalizar, chama a função que consome os dados do produtor de 30 em 30 segundos.
    while (not(producer.end_script)):
        consumer_function()
        time.sleep(30)
    end_consumer = True
    print("Consumidor terminou")
