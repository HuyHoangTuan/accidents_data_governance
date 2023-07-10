import pandas as pd
from config import CONFIG
from constants import CONST
from module import chicago_clean_data
import os
from sklearn.preprocessing import LabelEncoder

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
        path = '/'.join([CONFIG['RES_PATH'], 'cleaned', f'{prefix}.csv'])
        data_df = None
        for page_number in range(const['MIN_PAGE'], const['MIN_PAGE'] + const['NUM_PAGE']):
            df = read_data(prefix, page_number)
            df = filter_data(df, const['IGNORE_FEATURES'])
            chicago_clean_data(df)
            df = filter_data(df, const['FEATURES_REMOVED_POST_CLEAN'])
            if data_df is None:
                data_df = df
            else:
                pd.concat([data_df, df], ignore_index=True)
        # df = pd.read_csv(path, dtype=const['DATA_TYPES'])
        label_encoder = LabelEncoder()
        for column in data_df.columns:
            print(column)
            print(data_df[column].unique())
            data_df[column] = label_encoder.fit_transform(data_df[column])
        data_df.to_csv(path, index=False)
        
    print('Finish indexing...')
    