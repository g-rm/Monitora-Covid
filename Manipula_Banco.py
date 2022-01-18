from unidecode import unidecode
import psycopg2
import pandas as pd
import dask.dataframe as dd #To work with data that doesnot fit into memory
from sqlalchemy import create_engine
engine = create_engine('postgresql://gnux:102133@localhost:5432/covid_db_ufabc')
import datetime



#Estabelecendo conexão com o database
con = psycopg2.connect(host="localhost",
                       database="covid_db_ufabc",
                       user="gnux",
                       password="102133",
                       port=5432)

#Creating the cursor.
cur = con.cursor()

#Criando as tables, se não existirem
def Cria_tables():
    cur.execute('''CREATE TABLE IF NOT EXISTS vacinacao(document_id VARCHAR(255), paciente_id VARCHAR(255), paciente_idade VARCHAR(255), 
                paciente_dataNascimento VARCHAR(255), paciente_enumSexoBiologico VARCHAR(255), paciente_racaCor_codigo VARCHAR(255), 
                paciente_racaCor_valor VARCHAR(255), paciente_endereco_coIbgeMunicipio VARCHAR(255), paciente_endereco_coPais VARCHAR(255),
                paciente_endereco_nmMunicipio VARCHAR(255), paciente_endereco_nmPais VARCHAR(255), paciente_endereco_uf VARCHAR(255),
                paciente_endereco_cep VARCHAR(255), paciente_nacionalidade_enumNacionalidade VARCHAR(255), estabelecimento_valor VARCHAR(255),
                estabelecimento_razaoSocial VARCHAR(255), estalecimento_noFantasia VARCHAR(255), estabelecimento_municipio_codigo VARCHAR(255),
                estabelecimento_municipio_nome VARCHAR(255), estabelecimento_uf VARCHAR(255), vacina_grupoAtendimento_codigo VARCHAR(255), 
                vacina_grupoAtendimento_nome VARCHAR(255), vacina_categoria_codigo VARCHAR(255), vacina_categoria_nome VARCHAR(255),
                vacina_lote VARCHAR(255), vacina_fabricante_nome VARCHAR(255), vacina_fabricante_referencia VARCHAR(255), vacina_dataAplicacao VARCHAR(255),
                vacina_descricao_dose VARCHAR(255), vacina_codigo VARCHAR(255), vacina_nome VARCHAR(255), sistema_origem VARCHAR(255), data_importacao_rnds VARCHAR(255));''')

    
    cur.execute('''CREATE TABLE IF NOT EXISTS caso(date VARCHAR(255), state VARCHAR(255), city VARCHAR(255), place_type VARCHAR(255), confirmed VARCHAR(255),
                deaths VARCHAR(255), order_for_place VARCHAR(255), is_last VARCHAR(255), estimated_population_2019 VARCHAR(255), estimated_population VARCHAR(255), 
                city_ibge_code VARCHAR(255), confirmed_per_100k_inhabitants VARCHAR(255), death_rate VARCHAR(255));''')

    
    cur.execute('''CREATE TABLE IF NOT EXISTS internacional(iso_code VARCHAR(255), continent VARCHAR(255), location VARCHAR(255), date VARCHAR(255), total_cases VARCHAR(255),
                new_cases VARCHAR(255), new_cases_smoothed VARCHAR(255), total_deaths VARCHAR(255), new_deaths VARCHAR(255), new_deaths_smoothed VARCHAR(255),
                total_cases_per_million VARCHAR(255), new_cases_per_million VARCHAR(255), new_cases_smoothed_per_million VARCHAR(255), total_deaths_per_million VARCHAR(255),
                new_deaths_per_million VARCHAR(255), new_deaths_smoothed_per_million VARCHAR(255), reproduction_rate VARCHAR(255), icu_patients VARCHAR(255),
                icu_patients_per_million VARCHAR(255), hosp_patients VARCHAR(255), hosp_patients_per_million VARCHAR(255), weekly_icu_admissions VARCHAR(255),
                weekly_icu_admissions_per_million VARCHAR(255), weekly_hosp_admissions VARCHAR(255), weekly_hosp_admissions_per_million VARCHAR(255), new_tests VARCHAR(255),
                total_tests VARCHAR(255), total_tests_per_thousand VARCHAR(255), new_tests_per_thousand VARCHAR(255), new_tests_smoothed VARCHAR(255),
                new_tests_smoothed_per_thousand VARCHAR(255), positive_rate VARCHAR(255), tests_per_case VARCHAR(255), tests_units VARCHAR(255),
                total_vaccinations VARCHAR(255), people_vaccinated VARCHAR(255), people_fully_vaccinated VARCHAR(255), new_vaccinations VARCHAR(255),
                new_vaccinations_smoothed VARCHAR(255), total_vaccinations_per_hundred VARCHAR(255), people_vaccinated_per_hundred VARCHAR(255), 
                people_fully_vaccinated_per_hundred VARCHAR(255), new_vaccinations_smoothed_per_million VARCHAR(255), stringency_index VARCHAR(255), 
                population VARCHAR(255), population_density VARCHAR(255), median_age VARCHAR(255), aged_65_older VARCHAR(255), aged_70_older VARCHAR(255),
                gdp_per_capita VARCHAR(255), extreme_poverty VARCHAR(255), cardiovasc_death_rate VARCHAR(255), diabetes_prevalence VARCHAR(255), female_smokers VARCHAR(255),
                male_smokers VARCHAR(255), handwashing_facilities VARCHAR(255), hospital_beds_per_thousand VARCHAR(255), life_expectancy VARCHAR(255),
                human_development_index VARCHAR(255));''')



    
    
    con.commit()

#Load function for csv file that is ';' delimited
def load_csv1_to_db(file_name):#Neste, so subimos base que é separada por ;
    with open(file_name+'.csv', 'r') as f:
        # Notice that we don't need the `csv` module.
        next(f) # Skip the header row.
        cur.copy_from(f, file_name, sep=';')

    con.commit()
    
#Load function for csv file that is ',' delimited
def load_csv2_to_db(file_name):
    with open(file_name+'.csv', 'r', encoding='utf-8') as f:
        # Notice that we don't need the `csv` module.
        next(f) # Skip the header row.
        cur.copy_from(f, file_name, sep=',')

    con.commit()


def gera_vacina_grouped():
    
    #vamos começar criando uma table temporária contendo os dados agrupados de vacinacao
    query = '''
        SELECT REPLACE(paciente_endereco_uf, '"', '') AS paciente_uf, vacina_dataAplicacao, COUNT(paciente_id) AS qtd_vacinados
        INTO temp_vacinacao    
        FROM vacinacao
        GROUP BY paciente_uf, vacina_dataAplicacao'''

    cur.execute(query)
    con.commit()
    #Observe que a gente poderia se livrar da base 'vacinacao', mas vamos deixa-la lá, pois pode conter algum dado interessante pra analise
    
    #query pra formatar a coluna de datetime
    query = '''
        UPDATE temp_vacinacao
        SET 
            vacina_dataaplicacao = CAST(vacina_dataaplicacao AS DATE);'''
    cur.execute(query)
    con.commit()

def gera_hospital():
    df = pd.read_csv('hospital.csv')
    
    df.dataNotificacao = pd.to_datetime(df.dataNotificacao, infer_datetime_format=True)
    
    df.dataNotificacao = df.dataNotificacao.dt.strftime("%d/%m/%y")
    
    #SIM NO PROPRIO CSV TEM UMAS DATAS DE ANTES DA PANDEMIA (TEM ATÉ DO SÉCULO 20), OU SEJA, NÃO É BUG
    df.dataNotificacao = pd.to_datetime(df['dataNotificacao'], format="%d/%m/%y").dt.strftime("%Y-%m-%d")

    df = df.groupby(['dataNotificacao', 'estado']).agg({'ocupacaoSuspeitoUti':'sum', 'ocupacaoConfirmadoCli':'sum', 'ocupacaoConfirmadoUti':'sum',
                                                      'saidaSuspeitaObitos':'sum', 'saidaSuspeitaAltas':'sum', 'saidaConfirmadaObitos':'sum', 'saidaConfirmadaAltas':'sum'})
    
    df = df.reset_index()
    
    #Fazendo o de para pra criar a uf pra cruzar
    de_para_estado_uf = dict([['rondonia','RO'],['acre','AC'],['amazonas','AM'],['roraima','RR'],['para','PA'],['amapa','AP'],
                              ['tocantins','TO'],['maranhao','MA'],['piaui','PI'],['ceara','CE'],['rio grande do norte','RN'],
                              ['paraiba','PB'],['pernambuco','PE'],['alagoas','AL'],['sergipe','SE'],['bahia','BA'],['minas gerais','MG'],
                              ['espirito santo','ES'],['rio de janeiro','RJ'],['sao paulo','SP'],['parana','PR'],['santa catarina','SC'],
                              ['rio grande do sul','RS'],['mato grosso do sul','MS'],['mato grosso','MT'],['goias','GO'],['distrito federal','DF']])

        
    df['estado_uf'] = pd.Series(list(map(lambda x: de_para_estado_uf[unidecode(str(x)).lower()], df['estado'])))

    #Criando a chave que iremos usar nos cruzamentos
    df['chave'] = df['estado_uf']+df['dataNotificacao'].astype(str)

    #Estou descartando as informações sobre municipío, já que somente essa base teria. Mas caso se torne importante te-la,
    #poderia ser incluido no groupby, ao inves do estado 
    df.to_sql(name='hospital_por_estado', con=engine, if_exists='replace')
    
    

def Atualiza_table(table_name, df): #df recebe o dataframe que irá ser inserido ao dataframe. 
    #A gente remove ela, e depois sobe
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    
def Query_table(query):
    df = pd.read_sql_query(query, con=engine)
    
    con.commit()
    return df
    



#df_teste = pd.DataFrame([[1,2,3], [9,8,7]], columns=['id_cidade', 'nome', 'estado'])


Cria_tables()

#load_csv1_to_db('vacinacao')
#load_csv2_to_db('caso')
#load_csv2_to_db('internacional')
#gera_vacina_grouped()
#gera_hospital()

#Atualiza_table('cidade', df_teste)

#read_df = Query_table('SELECT * FROM cidade')

#cur.close()
#con.close()
