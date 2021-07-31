import pandas as pd
import itertools
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def create_games():
    """Create a DataFrame of all combinations games of LotoFácil.

    Returns:
        String: Conection for the database with all combinations of 15 numbers from 1 to 25 and the sum of this numbers
    """
 
    numbers_list = [x for x in range(1,26)] #numeros possíveis de jogar. 1 a 25

    all_games = list(itertools.combinations(numbers_list, 15)) #lista com todas as combinações possíveis

    df = pd.DataFrame(all_games,columns=['Bola1','Bola2','Bola3','Bola4','Bola5','Bola6','Bola7','Bola8','Bola9','Bola10','Bola11','Bola12','Bola13','Bola14','Bola15'])

    df['Soma'] = df.sum(axis=1)

    path = "C://Users//davil//projects//lottery_etl//data//"
    
    disk_engine = create_engine(f'sqlite:///{path}all_games.db')
    df.to_sql('all_games', disk_engine, if_exists='append') 
    
    con = "C://Users//davil//projects//lottery_etl//data//all_games.db"
    
    return con

def get_lottery_results():
    """Get the numbers from contests won.
    Returns:
       String: path for the csv_file with the numbers of the winning contests
    """

    url = "http://loterias.caixa.gov.br/wps/portal/loterias/landing/lotofacil/!ut/p/a1/04_Sj9CPykssy0xPLMnMz0vMAfGjzOLNDH0MPAzcDbz8vTxNDRy9_Y2NQ13CDA0sTIEKIoEKnN0dPUzMfQwMDEwsjAw8XZw8XMwtfQ0MPM2I02-AAzgaENIfrh-FqsQ9wBmoxN_FydLAGAgNTKEK8DkRrACPGwpyQyMMMj0VAcySpRM!/dl5/d5/L2dBISEvZ0FBIS9nQSEh/pw/Z7_HGK818G0K85260Q5OIRSC42046/res/id=historicoHTML/c=cacheLevelPage/=/"
    r = requests.get(url)
    bs = BeautifulSoup(r.text, 'html.parser')
    table = bs.find('table')
    df = pd.read_html(str(table))[0]
    df = df[['Concurso', 'Data Sorteio', 'Bola1', 'Bola2', 'Bola3', 'Bola4', 'Bola5',
             'Bola6', 'Bola7', 'Bola8', 'Bola9', 'Bola10', 'Bola11', 'Bola12',
             'Bola13', 'Bola14', 'Bola15']]
    
    df["Soma"] = df[['Bola1', 'Bola2', 'Bola3', 'Bola4', 'Bola5',
                     'Bola6', 'Bola7', 'Bola8', 'Bola9', 'Bola10', 'Bola11', 'Bola12',
                     'Bola13', 'Bola14', 'Bola15']].sum(axis = 1)
   
    path = "C://Users//davil//projects//lottery_etl//data//"

    df.to_csv(path+'result_games.csv',index = False) 
        
    return path+'result_games.csv'
    