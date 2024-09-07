import os
import pandas as pd
import PyPDF2
import asyncio
from groq import AsyncGroq
import re
import csv
from io import StringIO
import config

# Função para ler o texto de um PDF
def ler_pdf(caminho_pdf):
    with open(caminho_pdf, 'rb') as f:
        leitor_pdf = PyPDF2.PdfReader(f)
        texto = ""
        for pagina in range(len(leitor_pdf.pages)):
            texto += leitor_pdf.pages[pagina].extract_text()
    return texto

# Create the Groq client
async def consulta_groq(prompt, data):
    client = AsyncGroq(api_key=config.GROQ_API_KEY)

    chat_completion = await client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt + data
            }
        ],
        model="llama-3.1-70b-versatile",
            #llama3-8b-8192",
            max_tokens=6000,
            temperature=0.7,
            stream=False,
    )
    return chat_completion.choices[0].message.content

# Função para encontrar linhas que contêm "IOF" na descrição
def encontrar_dados_iof(df):
    return df[df['description'].str.contains('IOF', case=False, na=False)]
  
# Função para remover linhas que contêm a palavra "vencimento" na descrição
def remover_linhas_vencimento(df):
    return df[~df['description'].str.contains('vencimento', case=False, na=False)]

def clean_csv_data(input_text):
    # Encontrar o início do CSV (começando com "date,description,amount")
    match = re.search(r'date,description,amount.*', input_text, re.DOTALL)
    if match:
        csv_data = match.group(0)
        # Criar um DataFrame a partir dos dados limpos
        df = pd.read_csv(StringIO(csv_data))
        return df
    else:
        raise ValueError("Cabeçalho CSV não encontrado no texto de entrada.")

def create_xls(clean_csv_data, pdfs, formated_text):
    for index, item in enumerate(formated_text):
  # Criar um objeto StringIO para simular um arquivo
      data_io = StringIO(item)

      csv_reader = csv.reader(data_io, delimiter=',')
  # Ler o cabeçalho
      header = next(csv_reader)

  # Preparar os dados para o DataFrame
      rows = []
    
  # Criar o DataFrame
      try:
          df = clean_csv_data(item)
          print(df)
      
      # Salvar como arquivo Excel
          file_name = pdfs[index].removesuffix('.pdf') + '.xlsx'
          df.to_excel(file_name, index=False)
          print(f"Arquivo Excel {file_name} criado com sucesso.")
      except ValueError as e:
          print(f"Erro: {e}")
          
def formata_dados_pdf(consulta_groq, pdfs, prompt, dados_pdf):
    formated_text = []
    for index, item in enumerate(dados_pdf):
      print('Extraindo dados importantes: ' + pdfs[index])
      formated_text.append(asyncio.run(consulta_groq(prompt,item)))
      print(formated_text[index])
      # time.sleep(10)
    return formated_text
  
def le_varios_pdfs(ler_pdf, pdfs):
    dados_pdf = []
    for pdf in pdfs:
      print('Lendo pdf: ' + pdf)
      dados_pdf.append(ler_pdf(pdf))
    return dados_pdf

# Função para buscar arquivos PDF na pasta especificada
def buscar_arquivos(pasta, file_extension):
    # Lista todos os arquivos na pasta
    arquivos = os.listdir(pasta)
    
    # Filtra e retorna apenas os arquivos que terminam com ".pdf"
    files = [arquivo for arquivo in arquivos if arquivo.endswith(file_extension)]
    print("Arquivos encontrados:", files)
    return files

# Função para ler CSV e criar DataFrame
def ler_xlsx(caminho_csv):
    return pd.read_excel(caminho_csv)

# Função para comparar os valores 'amount' e separar iguais e diferentes
def comparar_e_separar(df_base, df_comparacao):
    df_iguais = df_base[df_base['amount'].isin(df_comparacao['amount'])]
    df_diferentes = df_base[~df_base['amount'].isin(df_comparacao['amount'])]
    df_exclusivos = df_comparacao[~df_comparacao['amount'].isin(df_base['amount'])]
    return df_iguais, df_diferentes, df_exclusivos
  
# Função para calcular a soma da coluna 'amount'
def calcular_soma(df):
    return df['amount'].sum()

# Função principal de mesclagem e comparação
def processar_e_comparar():
    # Ler CSVs fatura_diego e fatura_cris
    df_diego = ler_xlsx('fatura_diego.xlsx')
    df_cris = ler_xlsx('fatura_cris.xlsx')
    
    # Remover linhas que contêm "vencimento" na descrição
    df_diego = remover_linhas_vencimento(df_diego)
    df_cris = remover_linhas_vencimento(df_cris)

    # Mesclar os dois DataFrames
    df_diego_cris = pd.concat([df_diego, df_cris])
    
    # Ler o arquivo fatura_rafael
    df_rafael = ler_xlsx('fatura_rafael.xlsx')

    # Encontrar linhas com a palavra "IOF" na descrição
    df_iof_rafael = encontrar_dados_iof(df_rafael)
    df_rafael = df_rafael[~df_rafael['description'].str.contains('IOF', case=False, na=False)]
    

    # Concatenar os dados de IOF ao DataFrame df_diego_cris
    df_diego_cris = pd.concat([df_diego_cris, df_iof_rafael])
    
    # # Calcular a soma da coluna 'amount'
    # soma_amount = calcular_soma(df_diego_cris)

    # Salvar o arquivo combinado como Excel
    arquivo_excel = 'fatura_diego_e_cris.xlsx'
    with pd.ExcelWriter(arquivo_excel) as writer:
        df_diego_cris.to_excel(writer, sheet_name='fatura_original', index=False)
        # df_diego_cris.loc[:, 'amount'] = df_diego_cris['amount'].astype(float)  # Certificar que a coluna 'amount' é float
        # writer.sheets['fatura_original'].write_string(0, len(df_diego_cris.columns), f'Soma Total: {soma_amount}')

    print(f"Arquivo Excel {arquivo_excel} criado com sucesso com a mescla de fatura_diego e fatura_cris.")

    # Ler o arquivo fatura_rafael para comparação
    df_rafael = ler_xlsx('fatura_rafael.xlsx')

    # Comparar os valores 'amount'
    df_iguais, df_diferentes, df_exclusivos = comparar_e_separar(df_diego_cris, df_rafael)

    # Adicionar os dados iguais e diferentes em novas sheets no arquivo Excel
    with pd.ExcelWriter(arquivo_excel, mode='a') as writer:
        df_iguais.to_excel(writer, sheet_name='dados_iguais', index=False)
        df_diferentes.to_excel(writer, sheet_name='dados_diferentes', index=False)
        df_exclusivos.to_excel(writer, sheet_name='fatura_rafael', index=False)

    print(f"Análise completa. Dados iguais e diferentes adicionados ao arquivo {arquivo_excel}.")


# Caminho da pasta onde os PDFs estão localizados (pode ser a pasta atual, usando ".")
pasta_arquivos = '.'  # Use '.' para a pasta atual ou substitua pelo caminho desejado

# Buscar e exibir os nomes dos PDFs encontrados
arquivos_pdfs = buscar_arquivos(pasta_arquivos, '.pdf')

prompt = "Analise estes dados e capture apenas 'date' para a data da despesa, ex.: 14/04/2024 ou 14 AGO, 'description' para o nome da despesa e 'amount' para o valor da despesa, ex.: R$ -11,73 ou R$ 135,88 (remova o sinal negativo, remova o 'R$' e substitua a ',' por '.'). É extremamente importante que os valores 'amount' que possuírem a palavra 'estorno' na 'description' sejam convertidos para um número negativo, ou seja, se o valor for 135.38 e a 'description' dessa despesa contiver a palavra 'estorno' este 'amount' passará a ser -138.38. Todas as informações restantes devem ser ignoradas, inclusive a linha com o valor de vencimento da fatura. Responda SEMPRE no formato de csv: date,description,amoun (este formato deve SEMPRE estar no cabeçalho) e mais nada"

dados_pdf = le_varios_pdfs(ler_pdf, arquivos_pdfs)
formated_text = formata_dados_pdf(consulta_groq, arquivos_pdfs, prompt, dados_pdf)
create_xls(clean_csv_data, arquivos_pdfs, formated_text)

processar_e_comparar()