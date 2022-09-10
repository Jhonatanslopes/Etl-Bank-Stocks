from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
from pandas_datareader import data as web


def get_data():

   """
      function to get data from pandas_datareader.
      pandas_datareader returns stocks data

      return: stock data
   """
   
   # data period
   yesterday_date = datetime.now().date() - timedelta(1)
   start = yesterday_date
   end   = yesterday_date

   # dataframe to save data
   NUBR33 = pd.DataFrame()
   ITUB4 =  pd.DataFrame()
   BBAS3 =  pd.DataFrame()
   BBDC4 =  pd.DataFrame()
   SANB11 = pd.DataFrame()
   BPAC11 = pd.DataFrame()
   MODL11 = pd.DataFrame()
   BPAN4 =  pd.DataFrame()

   # get data from 8 banks 
   try: 
      NUBR33 = web.DataReader('NUBR33.SA', data_source='yahoo', start=start, end=end)
      ITUB4 = web.DataReader('ITUB4.SA', data_source='yahoo', start=start, end=end)
      BBAS3 = web.DataReader('BBAS3.SA', data_source='yahoo', start=start, end=end)
      BBDC4 = web.DataReader('BBDC4.SA', data_source='yahoo', start=start, end=end)
      SANB11 = web.DataReader('SANB11.SA', data_source='yahoo', start=start, end=end)
      BPAC11 = web.DataReader('BPAC11.SA', data_source='yahoo', start=start, end=end)
      MODL11 = web.DataReader('MODL11.SA', data_source='yahoo', start=start, end=end)
      BPAN4 = web.DataReader('BPAN4.SA', data_source='yahoo', start=start, end=end)

   # if there is no date in the date range
   except KeyError:
      raise Exception('date no data')

   # add column with bank code
   NUBR33['code'] = 'NUBR33'
   ITUB4['code'] =  'ITUB4'
   BBAS3['code'] = 'BBAS3'
   BBDC4['code'] = 'BBDC4'
   SANB11['code'] = 'SANB11'
   BPAC11['code'] = 'BPAC11'
   MODL11['code'] = 'MODL11'
   BPAN4['code'] = 'BPAN4'

   # concat datframes
   df_final = pd.concat([NUBR33, ITUB4, BBAS3, BBDC4, SANB11, BPAC11, MODL11, BPAN4], axis=0)
   return df_final.reset_index()

def connect_big_query(PROJECT_ID, KEY):

   '''
      function to connect with big query

      PROJECT_ID: project name -> str
      KEY:        file json with credentials -> json from bigquery

      return: connection big query, credential big query, project id
   '''

   # connect with big query
   credentials = service_account.Credentials.from_service_account_file(KEY)
   client = bigquery.Client(credentials= credentials,project=PROJECT_ID)

   return client, credentials, PROJECT_ID

def select_all(client, project_id, schema, table):

   """
      function to select data from big query

      client: connection with big query -> object bigquery
      project_id: project id -> str
      schema: dataset name -> str
      table: table name -> str

      return: all bigquery data
   """

   # execute query to show data 
   query_job = client.query(f"""
      SELECT * FROM {project_id}.{schema}.{table}
   """)

   # parse to datframe
   results = query_job.result()
   df = results.to_dataframe()

   print(df)
   return df

def insert_data(credentials, dataframe, project_id, schema, table):

   """
      functino to inset data into the big query

      credentials: credentials -> bigquery object
      dataframe: pandas_datareader data -> dataframe pandas
      project_id: project id -> str
      schema: dataset name -> str
      table: table name -> str 
   """

   # rename column to match database
   dataframe = dataframe.rename(columns={'Adj Close': 'Adj_Close'})

   # insert data
   dataframe.to_gbq(credentials=credentials,
                  destination_table=f'{project_id}.{schema}.{table}',
                  if_exists='append', table_schema=[
                     {'name': 'Date','type': 'DATE'},
                     {'name': 'High','type': 'FLOAT'},
                     {'name': 'Low','type': 'FLOAT'},
                     {'name': 'Open','type': 'FLOAT'},
                     {'name': 'Close','type': 'FLOAT'},
                     {'name': 'Volume','type': 'FLOAT'},
                     {'name': 'Adj_Close','type': 'FLOAT'},
                     {'name': 'code', 'type': 'STRING'}
                  ])

if __name__ == '__main__':

   data_to_save = get_data()
   client, credentials, project_id = connect_big_query('prject-etl', 'src/credentials.json')
   insert_data(credentials, data_to_save, project_id=project_id, schema='acoes', table='bancos')
   select_all(client=client, project_id=project_id, schema='acoes', table='bancos')
