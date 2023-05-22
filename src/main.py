import pandas as pd
from config import CONFIG
from constantce import CONST
from module import chicago_clean_data

def read_data(_prefix, index, is_raw=True):
    parent = 'raw' if is_raw else 'data'
    path = '/'.join([CONFIG['RES_PATH'], parent, _prefix])
    filename = f'{_prefix}.csv'
    if index is not None:
        filename = f'{_prefix}_{index}.csv'

    path = '/'.join([path, filename])
    return pd.read_csv(path)


def filter_data(table, ignore_features):
    return table.drop(columns=ignore_features)


if __name__ == '__main__':
    print('Start indexing...')
    for prefix in CONFIG['DATA_PREFIX']:
        const = CONST[prefix]
        for page_number in range(const['MIN_PAGE'], const['MIN_PAGE'] + const['NUM_PAGE']):
            df = read_data(prefix, page_number)
            df = filter_data(df, const['IGNORE_FEATURES'])
            chicago_clean_data(df)
    print('Finish indexing...')
