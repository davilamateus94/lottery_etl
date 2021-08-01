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
    """Get the numbers from won contests .

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
 

def count_odds_test(*args):
    """Count the odds numbers in a list of integers.

    Returns:
        Integer: Count of the odds numbers in a numbers list.
    """
    list_numbers = [arg for arg in args]
    odd_count = len(list(filter(lambda x: (x%2 != 0) , list_numbers)))
    return odd_count 

def count_sequence_6(*args):
    """Checks if determinated list have in maximum 6 numbers in sequence.
       For example:
       list1 = [1,2,3,4,5,6,10,11,12,17]
       list2 = [1,2,3,4,5,6,7,8,9,10]
        
       The list1 will return True and the list2 will return False.  
   
    Returns:
        Boolean: True if the list have in maximum 6 numbers in sequense, otherwise False .
    """

    retlist = []
    count = 1
    random_list = [arg for arg in args]
    
    # Avoid IndexError for  random_list[i+1]
    for i in range(len(random_list) - 1):
        # Check if the next number is consecutive
        if random_list[i] + 1 == random_list[i+1]:
            count += 1
        else:
            # If it is not append the count and restart counting
            retlist.append(count)
            count = 1
    # Since we stopped the loop one early append the last count
    retlist.append(count)
    
    filtered_numbers = [number for number in retlist if number > 6]
    
    if len(filtered_numbers) == 0:
        return True
    else:
        return False

def count_sequence_4(*args):
    """Checks if determinated list have in maximum 4 missing sequential numbers.
       For example:
       list1 = [1,2,3,4,5,6,10,11,12,17]
       list2 = [1,2,3,4,5,6,11,12,13,14]

        The list1 will return True and the list2 will return False. 

    Returns:
        Boolean: True if the list have in maximum 4 missing numbers in a sequential, otherwise False .
    """
    
    index_list = []
    random_list = [arg for arg in args]
    diferences_list = []
       # Avoid IndexError for  random_list[i+1]
    for i in range(len(random_list) - 1):
        # Check if the next number is consecutive
        if random_list[i] + 1 == random_list[i+1]:
            continue
        else:
            # If it is not append the index where the sequence was broken
            index_list.append(i)
            
    for i in range(len(index_list)):
       
        position = index_list[i]
        result = random_list[position +1] - random_list[position]
        diferences_list.append(abs(result))      

    filtered_numbers = [number for number in diferences_list if number > 4]
    
    if len(filtered_numbers) == 0:
        return True
    else:
        return False