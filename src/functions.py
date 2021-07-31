import pandas as pd
import numpy as np
import itertools

def create_games():
    """Create a DataFrame of all combinations games of LotoFácil.

    Returns:
        DataFrame: DataFrame with all combinations of 15 numbers from 1 to 25 and the sum of this numbers
    """
 
    numbers_list = [x for x in range(1,26)] #numeros possíveis de jogar. 1 a 25

    all_games = list(itertools.combinations(numbers_list, 15)) #lista com todas as combinações possíveis

    df_game = pd.DataFrame(all_games,columns=['Bola1','Bola2','Bola3','Bola4','Bola5','Bola6','Bola7','Bola8','Bola9','Bola10','Bola11','Bola12','Bola13','Bola14','Bola15'])

    df_game['Soma'] = df_game.sum(axis=1)

    return df_game

df = create_games()

print(df.head())