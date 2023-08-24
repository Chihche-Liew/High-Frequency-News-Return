import pandas as pd
import datetime as dt
import numpy as np
import duckdb

# for 2015 - 2022 .csv file
data_taq = pd.read_csv('./data/taq_2015.csv')
data_taq['datetime'] = pd.to_datetime(data_taq['datetime'], format='%d%b%Y:%H:%M:%S')
data_taq['sym_root'] = data_taq['SYM_ROOT']
data_taq['sym_suffix'] = data_taq['SYM_SUFFIX']
data_taq.drop(['SYM_ROOT', 'SYM_SUFFIX'], axis=1, inplace=True)

# for 2015 - 2022
link_tables = list([pd.read_pickle('./data/mastm_2015.pkl', compression='zip'),
                    # link taq to cusip
                    pd.read_pickle('./data/wrds_rpa_company_names.pkl', compression='zip'),
                    # link cusip to rp_entity_id
                    pd.read_pickle('./data/stocknames.pkl', compression='zip'),
                    # link shrcd and hexcd
                    pd.read_csv('./data/dse.csv')])
# link facpr and divamt

# for 2015 - 2022
# preprocess the taq data
data_taq['sym_suffix'] = pd.np.where(data_taq['sym_suffix'].isna(), 0, data_taq['sym_suffix'])
data_taq['symbol'] = data_taq['sym_root'] + data_taq['sym_suffix'].astype(str)
# fill the 9:45's NAs
data_taq = data_taq.sort_values(['symbol', 'datetime'])
data_taq['wvprice'] = data_taq.groupby('symbol')['wvprice'].transform(lambda x: x.fillna(method='ffill'))
# extract the date and time
data_taq['datetime'] = pd.to_datetime(data_taq['datetime'])
data_taq['date'] = data_taq.datetime.dt.date
data_taq['time'] = data_taq.datetime.dt.time
data_taq['year'] = data_taq.datetime.dt.year
data_taq['month'] = data_taq.datetime.dt.month
data_taq = data_taq.drop(data_taq[data_taq['time'] == dt.time(23, 59, 59)].index)
# dropping the stocks whose price < 1
filter_prc = data_taq['symbol'][data_taq['wvprice'] < 1].drop_duplicates()
data_taq = data_taq.loc[~(data_taq['symbol'].isin(filter_prc))]
data_taq.drop(['sym_root', 'sym_suffix'], axis=1, inplace=True)

# for taq 2015-2017
link_table_1 = pd.DataFrame(link_tables[0])
link_table_1['symbol_suffix'] = pd.np.where(link_table_1['symbol_suffix'].isna(), 0, link_table_1['symbol_suffix'])
link_table_1['symbol'] = link_table_1['symbol_root'] + link_table_1['symbol_suffix'].astype(str)
link_table_1 = link_table_1[['symbol', 'date', 'cusip']]
link_table_1['cusip'] = link_table_1['cusip'].astype(str).str[:8]
# merging cusip to taq
data_taq = data_taq.merge(link_table_1,
                          on=['date', 'symbol'],
                          how='left')
data_taq = data_taq.loc[~(data_taq['cusip'] == 'None')]

# for taq 2018-2022
key = data_taq[['symbol', 'sym_root', 'sym_suffix']].drop_duplicates()
key1 = key[key['sym_suffix'] == 0]
key = key[~key['sym_root'].isin(key1['sym_root'])]
avg_price = data_taq.groupby('symbol')['wvprice'].mean()
key = key.merge(avg_price, on='symbol')
key2 = key.loc[key['sym_root'].isin(key.groupby('sym_root')['wvprice'].idxmax().reset_index(0).iloc[:, 0])]
key = pd.concat([key1, key2])
data_taq = data_taq[data_taq['symbol'].isin(key['symbol'])]
link_table_1 = pd.DataFrame(link_tables[0])
link_table_1['sym_root'] = link_table_1['symbol_15']
link_table_1 = link_table_1[['sym_root', 'date', 'cusip']]
link_table_1['cusip'] = link_table_1['cusip'].astype(str).str[:8]
data_taq = data_taq.merge(link_table_1,
                          on=['date', 'sym_root'],
                          how='left')
data_taq = data_taq.loc[~(data_taq['cusip'] == 'None')]
data_taq.drop(['sym_root', 'sym_suffix'], axis=1, inplace=True)

link_table_2 = pd.DataFrame(link_tables[1])
link_table_2 = link_table_2[['rp_entity_id', 'cusip']].drop_duplicates(keep='first')
link_table_2['cusip'] = link_table_2['cusip'].astype(str).str[:8]
link_table_2 = link_table_2.loc[~(link_table_2['cusip'] == 'None')]
# merging rp_entity_id to taq
data_taq = data_taq.merge(link_table_2,
                          on='cusip',
                          how='left')

link_table_3 = pd.DataFrame(link_tables[2])
# merging permno to taq
data_taq = duckdb.query("""
SELECT a.*, b.permno
FROM data_taq as a
LEFT JOIN link_table_3 as b
ON a.cusip = b.cusip and a.date <= b.nameenddt and a.date >= b.namedt
""").df()
data_taq['permno'] = data_taq.groupby('cusip')['permno'].transform(lambda x: x.fillna(method='ffill'))
data_taq_1 = data_taq.loc[data_taq['permno'].notna()]
data_taq_2 = data_taq.loc[data_taq['permno'].isna()]
data_taq_2.drop('permno', axis=1, inplace=True)
data_taq_2 = duckdb.query("""
SELECT a.*, b.permno
FROM data_taq_2 as a
LEFT JOIN link_table_3 as b
ON a.cusip = b.ncusip and a.date <= b.nameenddt and a.date >= b.namedt
""").df()
data_taq = pd.concat([data_taq_1, data_taq_2])
data_taq.sort_values(['symbol', 'datetime'], inplace=True)
data_taq['permno'] = data_taq.groupby('cusip')['permno'].transform(lambda x: x.fillna(method='ffill'))

link_table_4 = pd.DataFrame(link_tables[2])
# merging hexcd and shrcd to taq
data_taq = duckdb.query("""
SELECT a.*, b.hexcd, b.shrcd
FROM data_taq as a
LEFT JOIN link_table_4 as b
ON a.permno = b.permno and a.date <= b.nameenddt and a.date >= b.namedt
""").df()

# filter: selecting the NYSE, NASDAQ, AMEX
data_taq = data_taq.loc[data_taq['hexcd'].isin([1, 2, 3])]
# filter: selecting the share code 10 and 11
data_taq = data_taq.loc[data_taq['shrcd'].isin([10, 11])]

link_table_5 = pd.DataFrame(link_tables[3])
link_table_5 = link_table_5[['date', 'permno', 'facpr', 'facshr', 'divamt']]
link_table_5.drop_duplicates(inplace=True)
link_table_5 = link_table_5.groupby(['permno', 'date'])['facpr', 'facshr', 'divamt'].sum().reset_index()
link_table_5['date'] = pd.to_datetime(link_table_5['date']).dt.date
data_taq['date'] = pd.to_datetime(data_taq['datetime']).dt.date
# merging facpr and divamt to taq
data_taq = data_taq.merge(link_table_5,
                          on=['permno', 'date'],
                          how='left')
data_taq.drop_duplicates(inplace=True, keep='first')
data_taq.sort_values(['symbol', 'datetime'], inplace=True)


def keep_first(group):
    group.iloc[1:, 14:] = np.nan
    return group


data_taq = data_taq.groupby(['permno', 'date']).apply(keep_first)


def first_to_zero(group):
    group.iloc[0, 14:] = 0
    return group


data_taq = data_taq.groupby('permno').apply(first_to_zero)
data_taq['facpr'].loc[(data_taq['facpr'].isna()) & ~(data_taq['divamt'].isna())] = 0
data_taq['facshr'].loc[(data_taq['facshr'].isna()) & ~(data_taq['divamt'].isna())] = 0
data_taq['divamt'].loc[~(data_taq['facpr'].isna()) & (data_taq['divamt'].isna())] = 0

# adjusting the price
data_taq.loc[data_taq['facpr'].notna() & data_taq['facpr'] != 0, 'divamt'] = 0
data_taq.loc[data_taq['busday_diff'] >= 2, 'wvprice'] = np.nan
data_taq['adj_prc'] = data_taq['wvprice']
data_taq['adj_prc'] = data_taq['adj_prc'] * (data_taq['facpr'] + 1)
data_taq['adj_prc'] = data_taq['adj_prc'] + data_taq['divamt']
data_taq.loc[data_taq['adj_prc'].isna(), 'adj_prc'] = data_taq['wvprice']
data_taq['adj_prc_sft'] = data_taq.groupby('permno')['adj_prc'].shift()
data_taq['prc_sft'] = data_taq.groupby('permno')['wvprice'].shift()
data_taq['adj_ret'] = np.where(~(data_taq['facpr'].isna()), data_taq['adj_prc'] / data_taq['adj_prc_sft'] - 1,
                               data_taq['wvprice'] / data_taq['prc_sft'] - 1)
data_taq.loc[data_taq['busday_diff'] >= 2, 'adj_ret'] = np.nan
# data_taq['adj_ret'] = data_taq.groupby('permno')['adj_prc'].apply(lambda x: x/x.shift() - 1)
# winsorization
# data_taq['adj_ret'] = data_taq.groupby(['symbol', 'date'])['adj_prc'].apply(lambda x: winsorize(x, limits=[0.01, 0.01]))

data_taq.to_csv('./data/output/taq_2015_cleaned.csv')
