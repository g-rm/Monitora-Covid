# -*- coding: utf-8 -*-
from PIL import ImageTk,Image
import tkinter as tk
import pandas as pd
from datetime import datetime

#Importando meus scripts agora
import Scrapper_Download_bases as scrapper
import Manipula_Banco as banco

#Importando as biblios necessarias para manipular o database
import psycopg2
import dask.dataframe as dd #To work with data that doesnot fit into memory
from sqlalchemy import create_engine
engine = create_engine('postgresql://gnux:102133@localhost:5432/covid_db_ufabc')

#Estabelecendo conexão com o database
con = psycopg2.connect(host="localhost",
                       database="covid_db_ufabc",
                       user="gnux",
                       password="102133",
                       port=5432)


def get_and_ShowQuery(entry1):  
    if type(entry1) != str:
        query = entry1.get("1.0",'end-1c') #A gente pega a query e fornece ela para o nosso banco de dados
     
    else:
        query = entry1
    #Cria a nova janela
    new_window = tk.Toplevel(root)
    canvas_resultado = tk.Canvas(new_window, width = 800, height = 400)
    canvas_resultado.pack()
    
    #Da um nome pra essa janela
    label_resultado = tk.Label(new_window, text='Resultado da query')
    label_resultado.config(font=('Arial Bold', 14))
    canvas_resultado.create_window(400, 12, window=label_resultado)


    print(query)
    '''Aqui entraria a chamada ao banco de dados, que vai retornar um dataframe'''
    df = banco.Query_table(query)
    
    #Ai, a gente abre uma nova tela, para mostrar os dados (head 100) e dar a opçao de baixar esses dados (completo)
    caixa_df = tk.Text(canvas_resultado)
    canvas_resultado.create_window(400, 200, window=caixa_df, width = 750, height = 350)
    caixa_df.insert(tk.END, str(df.head(10)))

    #Cria o botão pra baixar a base completa
    botao_baixar = tk.Button(canvas_resultado, text='Baixar Base Completa', command=(lambda x: x.to_csv('Resultado_query_{}.csv'.format(datetime.date(datetime.now()))))(df))
    canvas_resultado.create_window(725, 375, window=botao_baixar)


#Criando a tela de query
def query_window():

    query_window = tk.Toplevel(root)

    canvas1 = tk.Canvas(query_window , width = 400, height = 300)
    canvas1.pack()

    #Cria a caixa de texto de entrada
    entry1 = tk.Text(query_window ) 
    canvas1.create_window(200, 155, window=entry1, width = 350, height = 250)

    #Nome da tela
    label1 = tk.Label(query_window , text='Query Interface')
    label1.config(font=('Arial Bold', 14))
    canvas1.create_window(200, 14, window=label1)

    #Cria o botão pra enviar a query
    button1 = tk.Button(query_window, text='Submit Query', command=lambda: get_and_ShowQuery(entry1))
    canvas1.create_window(335, 270, window=button1)

    #Cria o botão pra atualizar o database (baixar novamente as bases e subir no database)
    Update_Database = tk.Button(query_window, text='Atualizar Database', command=(lambda x: print(x))('FUNÇÃO AINDA NÃO IMPLEMENTADA. FALTA CRIAR O DATABASE E O SCRIPT QUE INSERE E BUSCA NELE'))
    canvas1.create_window(78, 270, window=Update_Database)

#Aqui, iremos retornar nessa tela, a imagem da modelagem ER
def Tela_Info():
    info_window = tk.Toplevel(root)

    img = Image.open("modelo_ER.jpg")
    img = ImageTk.PhotoImage(img)
    panel = tk.Label(info_window, image=img)
    panel.image = img
    panel.pack()

def Dados_por_pais():
    #traz a base completa
    query = '''
    SELECT * 
    FROM internacional
    '''
    get_and_ShowQuery(query)

def Media_movel():
    query = '''
    SELECT *,
             avg(CAST(new_cases AS FLOAT)) OVER(
             PARTITION BY location
             ORDER BY date
             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
             AS week_avg
     
         FROM internacional WHERE new_cases <> '';
    '''
    get_and_ShowQuery(query)

def Top(ordenamento='total_cases'):
    query = '''
    SELECT I1.location, I1.date, I1.''' + ordenamento + ', I1.human_development_index' + '''
    FROM internacional I1
    LEFT OUTER JOIN internacional I2
        ON (I1.location = I2.location AND I1.''' + ordenamento + ' < I2.' + ordenamento + ')' + '''
    WHERE I2.location IS NULL AND I1.''' + ordenamento + " <> ''" + '''
    ORDER BY CAST(I1.''' + ordenamento + ' AS FLOAT) DESC;'

    get_and_ShowQuery(query)


def eficiencia():
    query = '''
    SELECT I1.location, I1.date, I1.total_cases, I1.total_deaths, 1 - CAST(I1.total_deaths AS FLOAT) / CAST(I1.total_cases AS FLOAT) AS eficiency, I1.human_development_index
    FROM internacional I1
      LEFT OUTER JOIN internacional I2
        ON (I1.location = I2.location AND I1.date < I2.date)
    WHERE I2.location IS NULL AND I1.total_deaths <> '' AND I1.total_cases <> ''
    ORDER BY 5 DESC;
    '''

    get_and_ShowQuery(query)

def Dados_por_estado():
    #traz a base completa, isto é, incluindo dados de vacinação e hospitais por estado
    
    #Pra poupar a ram, eu dividi a query em duas, usando bases temporarias.
    query = '''
    SELECT * 
    INTO temporary_caso_hosp
    FROM caso
    INNER JOIN hospital_por_estado as hosp
    ON hosp.chave = CONCAT(caso.state, caso.date)
    '''
    cur.execute(query)
    con.commit()
    
    query = '''
    SELECT * 
    FROM temporary_caso_hosp AS t1
    INNER JOIN temp_vacinacao AS t2
    ON t1.chave = CONCAT(t2.paciente_uf, t2.vacina_dataaplicacao)
    '''
    
    get_and_ShowQuery(query) 
    
    #Agora, limpa a base da memoria
    cur.execute('DROP TABLE temporary_caso_hosp')
    con.commit()


#Criando a tela principal
root = tk.Tk() #tela principal
canvas1 = tk.Canvas(root , width = 500, height = 400)
canvas1.pack()

#Nome da tela
label1 = tk.Label(root , text='Pagina Principal')
label1.config(font=('Arial Bold', 14))
canvas1.create_window(250, 14, window=label1)

#footer com nossos nomes
label2 = tk.Label(root , text='Um trabalho de')
label2.config(font=('Times', 8))
canvas1.create_window(330, 385, window=label2)

#footer com nossos nomes
label3 = tk.Label(root , text='Bruno, Gabriel e Felipe')
label3.config(font=('Times', 8))
canvas1.create_window(420, 385, window=label3)

#Botões para cada uma das funcionalidades:
#Cria o botão query mode
botao_query = tk.Button(root, text='Escrever query', command=query_window)
canvas1.create_window(100, 100, window=botao_query, height = 30, width = 100)

#Cria o botão infos sobre as bases
botao_info = tk.Button(root, text='Informações sobre as bases', command=Tela_Info)
canvas1.create_window(250, 100, window=botao_info, height = 30, width = 170)

#Cria o botão DE ALGUMA FUNÇÃO QUE AINDA NÃO PENSEI
botao_OUTRO = tk.Button(root, text='Eficiência', command=eficiencia)
canvas1.create_window(400, 100, window=botao_OUTRO, height = 30, width = 100)

#Cria o botão para query pais
botao_pais = tk.Button(root, text='Dados por pais', command=Dados_por_pais)
canvas1.create_window(100, 200, window=botao_pais, height = 30, width = 100)

#Cria o botão para dash pais e estado
botao_Dash_pais_estado = tk.Button(root, text='Dashboard pais e estado', command=(lambda x: x)(1))
canvas1.create_window(250, 200, window=botao_Dash_pais_estado, height = 30, width = 170)

#Cria o botão para estado
botao_query = tk.Button(root, text='Dados por estado', command=Dados_por_estado)
canvas1.create_window(400, 200, window=botao_query, height = 30, width = 100)

#Cria o botão query medias moveis
botao_query = tk.Button(root, text='Médias móveis', command=Media_movel)
canvas1.create_window(100, 300, window=botao_query, height = 30, width = 100)

#Cria o botão query mode
botao_query = tk.Button(root, text='Top valores', command=Top)
canvas1.create_window(400, 300, window=botao_query, height = 30, width = 100)




root.mainloop()
