import pandas
import numpy as np
import re
from copy import deepcopy
from multiprocessing import Pool
from functools import partial
import os
# np.random.seed(2017)

### get_LM_word_table: Get the language-meaning word table (entries are words)
def get_LM_word_table(filepath, Meanings):
    LM_word_table = pandas.DataFrame(columns = Meanings)
    #
    with open(filepath, 'r') as f:
        for line in f:
            if '{' in line:
                language = line.split('{')[0]
                LM_word_table = LM_word_table.append(pandas.DataFrame(index = [language], columns = Meanings))
                flag = 1
            else:
                if flag == 1:
                    flag = 0
                else:
                    # Get the first sound
                    lst = line.split()
                    if len(lst) < 3:
                        continue
                    meaning = lst[1]
                    word = re.sub('[~`!@#$%^&*-+/=:;.,?\|"]', '', lst[2])
                    if meaning in Meanings:
                        LM_word_table.loc[language, meaning] = word
    #
    return LM_word_table

### permute_LM_word: generate a permutation of LM_word_table
# How: in each language (row), permute all the words with the same length
def permute_LM_word(LM_word_table):
    def permute_row(LM_row):
        wordListLen = np.array([len(x) for x in LM_row.values]) # gives a 1-d numpy array
        uniq_len = np.unique(wordListLen)
        for length in uniq_len:
            if length >= 2:
                LM_row.loc[wordListLen == length,] = np.random.permutation(LM_row.loc[wordListLen == length,])
        return LM_row
    # notice this step changes the LM_word_table !
    LM_word_table = LM_word_table.apply(permute_row, axis = 1)
    return LM_word_table

### cntOccurrences: A function to generate a sound-meaning table
# (entries are the num of occurrences of a sound in a meaning) from an incidence/permutation of LM_word_table
# cntOccurrences() calls cntCol()
def cntCol(LM_col, Sounds): # notice the sequence of inputs here
    # Count the num of occurrences of difference sounds in this meaning (column)
    M = pandas.Series(0, index = Sounds, name = LM_col.name)
    # Loop through LM_col
    for word in LM_col:
        length = len(word)
        if length >= 2:
            wordL = [x[0]+x[1] for x in zip(word[0:length-1], word[1:length])]
            wordL = [x for x in wordL if x in Sounds]
            M.loc[wordL,] = M.loc[wordL,] + 1
            #
    return M

def cntOccurrences(Sounds, LM_word_table): # notice the sequence of inputs here
    SM_table = LM_word_table.apply(cntCol, axis = 0, args = (Sounds,))
    return SM_table

### parallelize_cntOccurrences: parallelize cntOccurrences
def parallelize_cntOccurrences(df, func, num_partitions, Sounds):
    df_split = np.array_split(df, num_partitions, axis = 1)
    pool = Pool(3)
    func = partial(func, Sounds)
    df = pandas.concat(pool.map(func, df_split), axis = 1)
    pool.close()
    pool.join()
    return df

### generate_permutations: generate permutations, and save into csv files, one file for each sound (symbol pair)
def generate_permutations(num_permutations, LM_word_table_copy, Sounds, Meanings):
    All_Permutations = pandas.DataFrame(columns = Meanings)
    #
    for i in range(num_permutations):
        LM_word_permuted = permute_LM_word(LM_word_table_copy)
        SM_table = parallelize_cntOccurrences(LM_word_permuted, cntOccurrences, 3, Sounds)
        All_Permutations = All_Permutations.append(SM_table)
        print("The " + str(i+1) + "th permutation completed")
        #
    for snd in Sounds:
        # Since Mac HDFS doesn't distinguish capital letters, it will mistake 'aG.csv', 'Ag.csv', 'AG.csv' for 'ag.csv', eg.
        # To avoid this, we need to encode the capital letters.  Here I put a '_' after each capital letter.
        processed_snd = ''.join(map(lambda x: x + '_' if str.isupper(x) else x, snd))
        filepath = './DataPermute_new/' + processed_snd + '.csv'
        All_Permutations.loc[snd,:].to_csv(filepath)
        #
    return

### add_permutations: add permutations to existing ones
def add_permutations(num_permutations, LM_word_table_copy, Sounds, Meanings):
    All_Permutations = pandas.DataFrame(columns = Meanings)
    #
    for i in range(num_permutations):
        LM_word_permuted = permute_LM_word(LM_word_table_copy)
        SM_table = parallelize_cntOccurrences(LM_word_permuted, cntOccurrences, 3, Sounds)
        All_Permutations = All_Permutations.append(SM_table)
        print("The " + str(i+1) + "th permutation completed")
        #
    for snd in Sounds:
        processed_snd = ''.join(map(lambda x: x + '_' if str.isupper(x) else x, snd))
        filepath = './DataPermute/' + processed_snd + '.csv'
        # append to existing csv file
        All_Permutations.loc[snd,:].to_csv(filepath, mode = 'a', header = False)
        #
    return



if __name__ == '__main__':
    Meanings = [
        'I', 'you', 'we', 'one', 'two', 'person', 'fish', 'dog', 'louse', 'tree', 'leaf', 'skin', 'blood', 'bone',
        'horn', 'ear', 'eye', 'nose', 'tooth', 'tongue', 'knee', 'hand', 'breast', 'liver', 'drink', 'see',
        'hear', 'die', 'come', 'sun', 'star', 'water', 'stone', 'fire', 'path', 'mountain', 'night', 'full',
        'new', 'name'
    ]

    Symbols = [
        'p', 'b', 'f', 'v', 'm', 'w', 't', 'd', 's', 'z', 'c', 'n', 'r', 'l', 'S', 'Z', 'C', 'j', 'T', 'y', 'k', 'g',
        'x', 'N', 'q', 'X', 'h', 'L', 'G', 'i', 'e', 'E', 'a', 'u', 'o'
    ]

    Sounds = [x + y for x in Symbols for y in Symbols]

    LM_word_table = get_LM_word_table('listss17.txt', Meanings)
    # The number of NaN in LM_word_table = 60372, accounts for 21% entries
    # NaN is distributed unevenly among Meanings. The missing rate varies from 10% ('eye') to 51% ('full')
    # In the ordinary test, this will affect the testing results a lot.

    ## Post-process LM_word_table: 1. change 'XXX' into NaN; 2. change NaN into '' (empty string)
    LM_word_table = LM_word_table.replace('XXX', np.NaN)
    LM_word_table = LM_word_table.applymap(lambda x: '' if pandas.isnull(x) else x)
    # Filter the languages whose total word length is 0.  6664 languages left.
    LM_word_length = LM_word_table.applymap(lambda x: len(x))
    tot_word_length = LM_word_length.apply(np.sum, axis = 1)
    LM_word_table = LM_word_table.loc[tot_word_length != 0,]

    ## Create a deepcopy of LM_word_table (LM_word_table_copy will be changed in permutations, LM_word_table stays the same)
    # LM_word_table_copy = deepcopy(LM_word_table)

    ##### Save the original count of Occurrences (original SM_table)
    cntOccurrences(Sounds, LM_word_table).to_csv('./DataOriginal/OriginalCnt.csv')

    ##### Save the length of each entry (word) in LM_word_table
    LM_word_length.loc[tot_word_length != 0,].to_csv('./DataOriginal/LM_word_length.csv')

    ##### Generate all permutations and get the SM_table for each permutation
    # save results into 1225 csv (one for each sound (symbol pair))
    generate_permutations(1000, LM_word_table, Sounds, Meanings)

    ##### Add permutations to existing data files
    # add_permutations(300, LM_word_table, Sounds, Meanings)
