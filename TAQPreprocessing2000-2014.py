import pandas as pd
import datetime as dt
import numpy as np
import duckdb

# for 2000 - 2014 .csv file
prev_data_taq = pd.read_csv('./data/taq_2002.csv')
prev_data_taq['datetime'] = pd.to_datetime(prev_data_taq['datetime'], format='%d%b%Y:%H:%M:%S')
prev_data_taq['symbol'] = prev_data_taq['SYMBOL']
prev_data_taq.drop('SYMBOL', axis=1, inplace=True)
prev_data_taq = prev_data_taq.groupby('symbol').tail(2)
prev_data_taq.reset_index(0, inplace=True)
data_taq = pd.read_csv('./data/taq_2003.csv')
data_taq['datetime'] = pd.to_datetime(data_taq['datetime'], format='%d%b%Y:%H:%M:%S')
data_taq['symbol'] = data_taq['SYMBOL']
data_taq.drop('SYMBOL', axis=1, inplace=True)
data_taq = pd.concat([prev_data_taq, data_taq])
data_taq.drop('index', axis=1, inplace=True)

# for 2000 - 2014
link_tables = list([pd.read_csv('./data/mastm_2000_2014.csv', encoding='ANSI'),
                    # link taq to cusip
                    pd.read_pickle('./data/wrds_rpa_company_names.pkl', compression='zip'),
                    # link cusip to rp_entity_id
                    pd.read_pickle('./data/stocknames.pkl', compression='zip'),
                    # link shrcd and hexcd
                    pd.read_csv('./data/dse.csv')])
# link facpr and divamt

# for 2000 - 2014
# fill the 9:45's NAs
data_taq = data_taq.sort_values(['symbol', 'datetime'])
# extract the date and time
data_taq['date'] = data_taq.datetime.dt.date
data_taq['time'] = data_taq.datetime.dt.time
data_taq['year'] = data_taq.datetime.dt.year
data_taq['month'] = data_taq.datetime.dt.month
data_taq['date_sft'] = data_taq.groupby('symbol')['date'].shift(fill_value=dt.date(1900, 1, 1))
data_taq['busday_diff'] = np.busday_count(data_taq['date_sft'].values.astype('<M8[D]'),
                                          data_taq['date'].values.astype('<M8[D]'))
data_taq['wvprice'] = data_taq.groupby('symbol')['wvprice'].transform(lambda x: x.fillna(method='ffill', limit=1))
data_taq = data_taq.drop(data_taq[data_taq['time'] == dt.time(23, 59, 59)].index)
data_taq = data_taq.drop('date_sft', axis=1)

# for taq 2000 - 2014
link_tables[0]['first_available_date'] = pd.to_datetime(link_tables[0]['first_available_date'], format='%Y%m%d').dt.date
link_tables[0]['last_available_date'] = pd.to_datetime(link_tables[0]['last_available_date'], format='%Y%m%d').dt.date
link_tables[0] = link_tables[0].groupby('SYMBOL').apply(
    lambda x: x.sort_values('last_available_date', ascending=True)).reset_index(drop=True)
link_tables[0]['first_available_date'] = link_tables[0].groupby('SYMBOL')['first_available_date'].apply(
    lambda x: x.fillna(dt.date(1900, 1, 1), limit=1) if x.isna().iloc[0] else x)
link_tables[0]['first_available_date'] = link_tables[0].groupby('SYMBOL')['first_available_date'].fillna(
    link_tables[0].groupby('SYMBOL')['last_available_date'].shift(1) + dt.timedelta(days=1))
link_tables[0] = link_tables[0].groupby('SYMBOL').apply(
    lambda x: x.sort_values('first_available_date', ascending=False)).reset_index(drop=True)
link_tables[0]['last_available_date'] = link_tables[0].groupby('SYMBOL')['last_available_date'].apply(
    lambda x: x.fillna(dt.date(2030, 1, 1), limit=1) if x.isna().iloc[0] else x)
link_tables[0]['last_available_date'] = link_tables[0].groupby('SYMBOL')['last_available_date'].fillna(
    link_tables[0].groupby('SYMBOL')['first_available_date'].shift(-1) - dt.timedelta(days=1))
link_tables[0]['cusip'] = link_tables[0]['CUSIP']
link_tables[0]['symbol'] = link_tables[0]['SYMBOL']
link_table_1 = link_tables[0]
data_taq = duckdb.query("""
SELECT a.*, b.cusip, b.symbol, b.first_available_date, b.last_available_date
FROM data_taq as a
LEFT JOIN link_table_1 as b
ON a.symbol = b.symbol and (a.date <= b.last_available_date) and (a.date >= b.first_available_date)
""").df()
data_taq['cusip'] = data_taq.groupby('symbol')['CUSIP'].transform(lambda x: x.fillna(method='ffill'))
data_taq.drop(['CUSIP', 'SYMBOL', 'first_available_date', 'last_available_date'], axis=1, inplace=True)
data_taq['cusip'] = data_taq['cusip'].astype(str).str[:8]

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

data_taq.to_csv('./data/output/taq_2003_cleaned.csv')
