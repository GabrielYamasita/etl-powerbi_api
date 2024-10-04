import os
import glob
import msal
import requests
import json

class Extract():

    # Iniciando os parâmetros necessários para autenticação, identificação do Dataset, pasta onde serão gravados os dados e pasta contendo as consultas DAX
    def __init__(self, client_id, client_secret, group_id, dataset_id, authority, scope, dir_ext, dir_queries):
        self.client_id = client_id
        self.client_secret = client_secret
        self.group_id = group_id
        self.dataset_id = dataset_id
        self.authority = authority
        self.scope = scope
        self.dir_ext = dir_ext
        self.dir_queries = dir_queries


    def get_acess_token(self):
        # Autenticando
        app = msal.ConfidentialClientApplication(self.client_id, authority=self.authority, client_credential=self.client_secret)
        token_response = app.acquire_token_for_client(scopes=self.scope)

        # Verificando se o token foi obtido
        if 'access_token' in token_response:
            print("Token de acesso obtido com sucesso!")
            access_token = token_response['access_token']

            return access_token
        else:
            raise Exception("Falha ao obter o token: " + token_response.get("error_description"))


    def run_query(self, query, access_token, file):
        # URL para a consulta ao dataset no Power BI
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/executeQueries"

        # Cabeçalhos da solicitação
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        # Fazer a solicitação POST para executar a consulta
        response = requests.post(url, headers=headers, json=query)

        # Verificar a resposta
        if response.status_code == 200:
            print("Consulta executada com sucesso!")
            result = response.json()
            
            # Gerar o resultado em um arquivo .json
            with open(f'{file}.json', 'w') as f:
                json.dump(result, f, indent=4)
            
            print("Resultado salvo como resultado_consulta.json")
        else:
            print("Falha ao executar a consulta:", response.status_code, response.text, str(response))    


    # Formata a consulta para execução via API
    def format_query(self, query):
        formatted_query = {
            "queries": [
                {
                    "query": query
                }
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }
        
        return formatted_query    


    # Função que procura por consultas DAX dispostas em arquivos .txt.
    # Cada consulta é atribuída a um dicionário cuja chave será o nome do arquivo txt, e o valor será a consulta como texto.
    def search_queries(self):

        # Verifica se o diretório existe
        if not os.path.exists(self.dir_queries):
            print(f"O diretório especificado não existe: {self.dir_queries}")
        else:
            print(f"O diretório especificado existe: {self.dir_queries}")

        # Lista todos os arquivos no diretório para depuração
        print("Arquivos no diretório:")
        print(os.listdir(self.dir_queries))

        # Inicializa um dicionário para armazenar as consultas
        queries_dict = {}

        # Encontra todos os arquivos .txt no diretório especificado
        files = glob.glob(os.path.join(self.dir_queries, "*.txt"))

        # Verifica se encontrou arquivos .txt
        if not files:
            print("Nenhum arquivo .txt encontrado no diretório especificado.")
        else:
            print(f"Arquivos .txt encontrados: {files}")

        # Itera sobre cada arquivo encontrado
        for file in files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    # Lê o conteúdo do arquivo e armazena no dicionário
                    file_name = os.path.basename(file)  # Pega o nome do arquivo para usar como chave
                    queries_dict[file_name] = f"""
        {f.read()}
        """         
                        
            except FileNotFoundError as e:
                print(f"Erro ao tentar abrir o arquivo {file}: {e}")

        # Agora o dicionário consultas_dax contém todas as consultas DAX, se os arquivos foram lidos corretamente
        return queries_dict    

    # Executa a consulta e salva os dados em formato JSON na pasta referenciada
    def run_queries_and_save(self, query, access_token, file_name):
        # URL para a consulta ao dataset no Power BI
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.group_id}/datasets/{self.dataset_id}/executeQueries"

        # Cabeçalhos da solicitação
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        try:

            # Fazer a solicitação POST para executar a consulta
            response = requests.post(url, headers=headers, json=query)
            
            # Verificar a resposta
            if response.status_code == 200:
                print("Consulta executada com sucesso!")
                result = response.json()
                
                # Gerar o resultado em um arquivo .json
                with open(f'{file_name}.json', 'w') as f:
                    json.dump(result, f, indent=4)
                
                print("Resultado salvo como resultado_consulta.json")
            else:
                print("Falha ao executar a consulta:", response.status_code, response.text, str(response))   
        except Exception as e:
           print(f"Ocorreu um erro: {e}") 
