import os
import msal
import requests
import json
import glob
import traceback
import sys 

from supabase import create_client, Client
from dotenv import load_dotenv

sys.path.insert(0, "..\\lib")

from extract import Extract
from transform import Transform

# Carregar as variáveis de ambiente
load_dotenv()

# Diretório onde serão gravados os dados extraídos
json_data = '..\\data'

# Diretório onde estão as consultas DAX em arquivos txt
dax_queries = '..\\queries' 

# Executa a extração dos dados no Power BI gravando-os em arquivos JSON
def run_extraction():

    # Variável contendo o dicionário com consultas dax
    consultas = {}

    # Informações para autenticação e geração do token para API
    client_id_app : str = os.environ.get("CLIENT_ID")
    client_secret_app : str = os.environ.get("CLIENT_SECRET")
    tenant_id : str = os.environ.get("TENANT_ID")

    # ID do Workspace e Dataset em que serão executadas as consultas
    group_id_app : str = os.environ.get("GROUP_ID")
    dataset_id_app : str = os.environ.get("DATASET_ID")

    # Endpoint da API do Azure AD para obter o token
    authority = f"https://login.microsoftonline.com/{tenant_id}"

    # Escopo de permissões
    scope = ["https://analysis.windows.net/powerbi/api/.default"]

    # Construtor da classe de extração em nossa lib
    ext = Extract(
        client_id = client_id_app,
        client_secret = client_secret_app,
        group_id = group_id_app,
        dataset_id = dataset_id_app,
        authority = authority,
        scope = scope,
        dir_ext = json_data,
        dir_queries = dax_queries
    )

    # Gerando token de acesso
    token_acesso = ext.get_acess_token()

    # Buscando e inserindo no dicionário as consultas
    consultas = ext.search_queries()

    # Para cada arquivo txt contendo uma consulta DAX, será feito a execução desta consulta no Dataset através da API e token de acesso
    for nome_arquivo, consulta in consultas.items():

        # Retirando o ".txt" do nome do arquivo
        tabela = nome_arquivo.replace('.txt','')

        # Formatando a consulta para o formato JSON esperado, jogando o DAX como valor da chave 'query'
        consulta_formatada = ext.format_query(consulta)

        # Executar a consulta formatada e gravar os dados como JSON na pasta
        ext.run_queries_and_save(consulta_formatada, token_acesso, f'{json_data}/{tabela}')

# Executa as transformações e carga dos dados
def run_transform_load():

    # Obtendo a URL e chave da API para o Supabase
    url : str = os.environ.get("SUPABASE_URL")
    key : str = os.environ.get("SUPABASE_KEY")

    try:

        # Criando a conexão com API da Supabase
        supabase : Client = create_client(url, key)

        # Obtendo em uma lista todos os arquivos JSON na pasta
        file_dir = glob.glob(os.path.join(json_data, '*.json'))

        # Substituindo os elementos de diretório dos itens da lista para manter somente o nome do arquivo, usando list comprehension e replace
        files = [item.replace('..\\data', '') for item in file_dir]

        # Percorrendo a lista de arquivos
        for file in files:
            
            # Removendo a extensão do arquivo
            table = file.replace('.json', '')

            # Iniciando construtor da classe com as transformações
            transform = Transform()

            # Transformando os dados (não terá tanta enfâse aqui por questões de segurança, para não expor os dados)
            new_table = transform.exemplo_sua_transformacao(table)

            # Imprimir para acompanhamento
            print(f'\nIniciando a carga da tabela {table}')

            # Formata de string para JSON, conforme o padrão esperado pelo Insert do Supabase no Python
            data = json.loads(new_table)

            # Chamando API do Supabase para execução do Insert no banco
            response = (
                supabase.table(table) # Mesmo nome de tabela no Supabase para padronização e coerência
                .insert(data)
                .execute()
            )

            # Por padrão o response do Supabase só retorna o JSON que foi inserido pelo Insert, como pode "bagunçar" a exibição na tabela ou no log (em produção está rodando os prints no Airflow), 
            # não está sendo feito nada com o response.
            print(f'Concluída inserção da tabela {table}')

    except Exception as exception:
        print('\n')
        traceback.print_exc()


def main():
    run_extraction()
    run_transform_load()

if __name__ == "__main__":
    main()