import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import json

def split_csv(csv_path):
    file = open('divisions.txt')
    partitions = file.read()
    file.close()
    for chunk in pd.read_csv(csv_path, chunksize=1000000, low_memory=False):
        for nbr, char in enumerate(partitions):
            path_save = 'chunk-'+ str(nbr) +'.csv'
            chunk[chunk['msno'].str[0] == char].to_csv(path_save, index=False, mode='a')


def clean_aggregate_members():
    for nbr in range(64):
        df = pd.read_csv('members-chunk-'+ str(nbr) +'.csv', usecols=['msno', 'bd', 'gender', 'registered_via'])
        df.loc[:,'bd'] = pd.to_numeric(df['bd'], errors='coerce')
        df = df.fillna(0)
        df.loc[~df['bd'].between(10, 80), 'bd'] = 0
        df = df.drop(index=df[df['registered_via']=='registered_via'].index)
        df = df.drop(index=df[df['gender']=='gender'].index)
        age_ranges = np.linspace(-10, 80, num=10)
        df.loc[:,'bd'] = pd.cut(df['bd'], bins=age_ranges, retbins=True, include_lowest=True, labels=age_ranges[1:])[0].astype(np.int16)
        df.to_csv('members_agg-chunk-'+str(nbr)+'.csv', index=False)


def dateparser(date_str):
    try:
        result = datetime.strptime(date_str, '%Y%m%d')
    except:
        result = np.nan
    return result


def aggregate_user_logs_by_month():
    for nbr in range(64):
        df = pd.read_csv('user_logs_chunk-'+str(nbr)+'.csv', 
                usecols=['msno', 'date', 'num_25'], low_memory=False,  
                parse_dates=['date'], date_parser=dateparser)
        df = df.drop(index=df[df['msno']=='msno'].index)
        df = df.dropna(subset=['date'])
        df = df.groupby(by=['msno', pd.Grouper(key='date', freq='M')])['num_25'].count() 
        df = df.reset_index()
        df = df.rename(columns={'num_25': 'daily_usage'})
        df.to_csv('user_logs_agg_month-chunk-'+str(nbr)+'.csv', index=False)


def aggregate_user_logs_by_id():
    for nbr in range(64):
        df = pd.read_csv('user_logs_agg_month-chunk-'+str(nbr)+'.csv')
        df = df.groupby(by=['msno'])['daily_usage'].mean()
        df = df.reset_index()
        df = df.rename(columns={'daily_usage': 'avg_usage'})
        df.to_csv('user_logs_avg-chunk-'+str(nbr)+'.csv', index=False)


def avg_usage():
    df = pd.DataFrame()
    for nbr in range(64):
        df_m = pd.read_csv('members_agg-chunk-'+str(nbr)+'.csv', index_col='msno')
        df_user = pd.read_csv('user_logs_avg-chunk-'+str(nbr)+'.csv', index_col='msno')
        df_m = df_m.join(df_user, on='msno', how='inner')
        df_m = df_m.reset_index(drop=True)
        df = df.append(df_m)
    df = pd.pivot_table(df, values=['avg_usage'], index=['bd', 'gender', 'registered_via'],
                            aggfunc=['mean', len])
    df.columns = df.columns.droplevel(1)
    df = df.rename(columns={'mean': 'avg_usage',
                            'len': 'nbr_users'})
    df = df.reset_index()


def cohorts_by_month():
    repl_dict = json.load(open("replacement_dict.txt"))
    dfn = pd.read_csv('groups.csv', index_col='idx', usecols=['nbr', 'idx'])
    dff = pd.DataFrame()
    for nbr in range(64):
        dfm = pd.read_csv('members_agg-chunk-'+str(nbr)+'.csv', index_col='msno')
        for col in ['bd', 'gender', 'registered_via']:
            dfm.loc[:, col] = dfm[col].replace(repl_dict.get(col))
        dfm['idx'] = dfm['bd'].map(str) + dfm['gender'].map(str) + dfm['registered_via'].map(str)
        dfm.loc[:, 'idx'] = pd.to_numeric(dfm['idx'])
        dfm = dfm.reset_index()
        dfm = dfm.set_index(['idx'])
        dfm = dfm.join(dfn, on='idx', how='left')
        dfm = dfm.set_index(['msno'])
        dfm = dfm[['nbr']]
        dfu = pd.read_csv('user_logs_agg_month-chunk-'+str(nbr)+'.csv', index_col='msno')
        dfm = dfm.join(dfu, on='msno', how='inner')
        dfm = dfm.reset_index(drop=True)
        dff = dff.append(dfm)
    table = pd.pivot_table(dff, values=['daily_usage'], index=['date', 'nbr'],
                           aggfunc=['mean'])
    table.to_csv('table_month.csv')






