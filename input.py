import pandas as pd
import os
import mysql.connector
from mysql.connector import Error

arquivos = r"./Movimentações//"
arquivosRedeAmil = r"./RedeAmil//"

def tratarExel():
    for file_name in os.listdir(arquivos):
        name_file = os.path.join(arquivos, file_name)

        df = pd.read_excel(name_file)

        # Remove linhas onde a primeira coluna não é numérica
        df = df[pd.to_numeric(df.iloc[:, 0], errors='coerce').notnull()]

        # Capitaliza a segunda coluna
        df.iloc[:, 1] = df.iloc[:, 1].apply(lambda x: str(x).capitalize() if isinstance(x, str) else x)

        # Define o cabeçalho
        header = [
            "carteirinha", "beneficiario", "matricula", "cpf", "plano", "titularidade",
            "idade", "dependencia", "data limite", "data inclusão", "data exclusão",
            "lotacao", "status", "co-participacao", "outros", "mensalidade", "total familia"
        ]

        df.columns = header

        # Salva o DataFrame tratado de volta para o Excel
        df.to_excel(name_file, index=False)

def enviarDadosParaOBancoAmil():
    conexao = None  # Inicializa a variável 'conexao' como None

    for file_name in os.listdir(arquivos):
        name_file = os.path.join(arquivos, file_name)

        df = pd.read_excel(name_file)
        df = df.fillna('Vazio')

        # Verifica e converte colunas de data
        df['data limite'] = pd.to_datetime(df['data limite'], format='%d/%m/%Y', errors='coerce')
        df['data inclusão'] = pd.to_datetime(df['data inclusão'], format='%d/%m/%Y', errors='coerce')
        df['data exclusão'] = pd.to_datetime(df['data exclusão'], format='%d/%m/%Y', errors='coerce')

        # Verifica se há valores nulos nas colunas de data
        if df[['data limite', 'data inclusão', 'data exclusão']].isnull().any().any():
            print(f"Datas inválidas encontradas no arquivo: {file_name}")

        try:
            # Configurar a conexão
            conexao = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="root",
                password="",
                database="w3g_movimentacoes"
            )

            cursor = conexao.cursor()

            # Consulta SQL para inserção
            query = """
                REPLACE INTO movimentacoes(
                    carteirinha, beneficiario, matricula, cpf, plano, titularidade, 
                    dependencia, data_limite, data_inclusão, data_exclusão, lotacao, 
                    status, co_participacao, outros_gastos, mensalidade, mensalidade_familia)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for index, row in df.iterrows():
                dados = (
                    row['carteirinha'], row['beneficiario'], row['matricula'], row['cpf'],
                    row['plano'], row['titularidade'], row['dependencia'],
                    row['data limite'], row['data inclusão'], row['data exclusão'],
                    row['lotacao'], row['status'], row['co-participacao'], row['outros'],
                    row['mensalidade'], row['total familia']
                )
                cursor.execute(query, dados)

            conexao.commit()

        except Error as e:
            print("Erro ao conectar ao MySQL:", e)

        finally:
            # Verifica se a conexão foi estabelecida antes de tentar fechá-la
            if conexao and conexao.is_connected():
                conexao.close()
                print("Conexão Encerrada")

def enviarRedeAmil():
    conexao = None  # Inicializa a variável 'conexao' como None

    for nameFile in os.listdir(arquivosRedeAmil):
        fileName = os.path.join(arquivosRedeAmil, nameFile)

        df = pd.read_excel(fileName, sheet_name=None)

        try:
            conexao = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="root",
                password="",
                database="w3g_movimentacoes"
            )

            cursor = conexao.cursor()

            query = """
                REPLACE INTO redeAmil(
                    codigo_rede, nome_rede, uf, municipio, elemento_de_divulgação, nome_prestador,
                    endereco, numero, complemento, bairro, cep, ddd, telefone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for sheet_name, df_sheet in df.items():
                df_sheet = df_sheet.fillna('Vazio')
                for index, row in df_sheet.iterrows():
                    dados = (
                        row['Código da Rede'], row['Nome da Rede'], row['UF'],
                        row['Municipio'], row['Elemento de Divulgação'], row['Nome do Prestador'],
                        row['Endereço Prestador'], row['Número'], row['Complemento'], row['Bairro'],
                        row['CEP'], row['DDD Telefone 1'], row['Telefone 1']
                    )
                    cursor.execute(query, dados)

            conexao.commit()

        except Error as e:
            print("Erro ao conectar ao MySQL:", e)

        finally:
            if cursor:
                cursor.close()
            if conexao and conexao.is_connected():
                conexao.close()

def enviarReembolso():
    conexao = None  # Inicializa a variável 'conexao' como None

    for nameFile in os.listdir(arquivosRedeAmil):
        fileName = os.path.join(arquivosRedeAmil, nameFile)

        df = pd.read_excel(fileName, sheet_name=None)

        try:
            conexao = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="root",
                password="",
                database="w3g_movimentacoes"
            )

            cursor = conexao.cursor()

            #  ESTÁ FALTANDO A PRIMEIRA COLUNA

            # Consulta SQL para inserção
            query = """
                    REPLACE INTO reembolso(
                        codigo, Amil_S380, Amil_S450, Amil_S580, Amil_S750_R1, Amil_S750_R2, 
                        Amil_S750_R3, Amil_ONE_S1500_R1, Amil_ONE_S1500_R2, Amil_ONE_S2500_R1, Amil_ONE_S2500_R1, 
                        Amil_ONE_S2500_R2, Amil_ONE_S6500_Black_R1, Amil_ONE_S6500_Black_R2, Amil_ONE_S6500_Black_R3)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

            for index, row in df.iterrows():
                dados = (
                    row['carteirinha'], row['beneficiario'], row['matricula'], row['cpf'],
                    row['plano'], row['titularidade'], row['dependencia'],
                    row['data limite'], row['data inclusão'], row['data exclusão'],
                    row['lotacao'], row['status'], row['co-participacao'], row['outros'],
                    row['mensalidade'], row['total familia']
                )
                cursor.execute(query, dados)

            conexao.commit()

        except Error as e:
            print("Erro ao conectar ao MySQL:", e)

        finally:
            # Verifica se a conexão foi estabelecida antes de tentar fechá-la
            if conexao and conexao.is_connected():
                conexao.close()
                print("Conexão Encerrada")

if __name__ == '__main__':
    enviarRedeAmil()
