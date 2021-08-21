import pandas as pd
import itertools
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def create_games(path):
    """Create a DataFrame of all LotoFácil combinations games and save it in a parquet file.

    Args:
        path (string): path where you desire to save the file.

    Returns:
        string: Path where the parquet files were saved.
    """
    spark = SparkSession.builder \
        .master("local[8]") \
        .config('spark.executor.memory', '16g') \
        .config('spark.driver.memory', '16g') \
        .config("spark.driver.maxResultSize", "2048MB") \
        .config("spark.port.maxRetries", "100") \
        .appName("Learning_Spark") \
        .getOrCreate()   

    df_game = spark.createDataFrame(list(itertools.combinations([x for x in range(1,26)], 15)),  ['Bola1','Bola2','Bola3','Bola4','Bola5',
                                                                                                  'Bola6','Bola7','Bola8','Bola9','Bola10',
                                                                                                  'Bola11','Bola12','Bola13','Bola14','Bola15'])
        
    df_game = df_game.withColumn('Soma', sum(df_game[col] for col in df_game.columns))
    
    df_game = df_game.withColumn('Bolas_Array', f.array('Bola1','Bola2','Bola3','Bola4','Bola5',
                                                        'Bola6','Bola7','Bola8','Bola9','Bola10',
                                                        'Bola11','Bola12','Bola13','Bola14','Bola15'))
       
    df_game.write.parquet(path + "all_games", mode="overwrite")
       
    return path + "all_games"

def get_lottery_results(path):
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
   
    
    df.to_csv(path +'result_games.csv',index = False) 
        
    return path +'result_games.csv'

   
def count_odds(list_numbers):
    """Count the odds numbers in a list of integers.

    Args:
        list_numbers (array): Array in a DataFrame with contains the number witch you desire to count the odds.

    Returns:
        Integer: Count of the odds numbers in a numbers list.
    """

    odd_count = len(list(filter(lambda x: (x%2 != 0) , list_numbers)))
    return odd_count 

def return_parquet_odds(path,df,array_column):
    """Create a parquet file with a column where indicate the count of odd numbers in determinated vector.

    Args:
        path (str): path where you desire to save the file.
        df (dataframe): data with you want to evaluate
        array_column (dataframe column): column with the vectors that you desired to count the odds.

    Returns:
        string: path where the parquet files were saved.
    """

    count_odds_udf = f.udf(lambda x: count_odds(x)) 
    
    df = df.withColumn('impares', count_odds_udf(f.col(array_column)))
  
    df.write.parquet(path + "with_odds", mode="overwrite")
    
    return path + "with_odds" 


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

def get_last_result(df):
    """Return las line of a given dataframe.
       This last line  contains the most recent result of the lottery game.

    Args:
        df (DataFrame): Dataframe containing lottery results

    Returns:
        list: containing the results of the last contest
    """

    last_result = df.tail(1).reset_index(drop = True)
    
    return list(last_result.iloc[0,2:17])


def generating_combinations(numbers_list, number_combinations):
    """Return combinations of elements from a given list.

    Args:
        numbers_list (list): containing the results of the last contest
        number_combinations (int): number of combinations in the list.
    
    Returns:
        list: return a list
    """
    
    combinations = list(itertools.combinations(numbers_list, number_combinations))
    final_combinations = [x for x in combinations]
    
    return final_combinations

def issubset_last_result(combinations_result,result,*args):
    """Check if the (N) combinations from the most recent lottery result are framed in the combinations that will be evaluated.
       For example:
       Last Result = [3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]
       C 15,8 = [3,4,5,6,7,8,9,10]
       Evaluate_combination = [1,2,3,4,5,6,7,8,9,10,18,19,20,21,22]
       
       This analysis will return True because we have in maximum 8 numbers from C 15,8 in the evaluate combination. 

    Args:
        combinations_result (list): list of combinations of results taken by a user-defined number
        result (list): list with the numbers of the last lottery won

    Returns:
        boolean : True if the combination of the last results match the analyzed combination otherwise False.
    """

    random_list = args
    for elemen in combinations_result:
        if (set(elemen).issubset(random_list) == True) and (len(set(set(result).symmetric_difference(elemen)).intersection(random_list)) == 0):
            return True
    return False

def identify_patters(line,*args):
    """Count the number of times that numbers in a list appear in determinated sequence.
       For example:
       row1 = [1,2,3,4,5]
       row2 = [6,7,8,9,10]
       row3 = [11,12,13,14,15]
       row4 = [16,17,18,19,20]
       row5 = [21,22,23,24,25]
       seq = [1,2,3,4,5,8,12]
       The code will return a count of 5 for the first line 1 for the seccond line, 1 for the third line.
       
    Args:
        line (list): count of times the numbers on the list to be evaluated in the sequence appeared.

    Returns:
        interger: numbers 
    """

    return len(set(args).intersection(line))

