import pandas as pd
import datetime as dt
import numpy as np
import duckdb

data_taq = pd.read_csv('./data/taq_2002_cleaned.csv')
data_taq.drop('Unnamed: 0', axis=1, inplace=True)

data_ravenpack = pd.read_pickle('./data/dj_equities_2002.pkl', compression='zip')

data_ravenpack['timestamp_utc'] = pd.to_datetime(data_ravenpack['timestamp_utc'])
data_ravenpack['time'] = data_ravenpack.timestamp_utc.dt.time
data_ravenpack.loc[(data_ravenpack['time'] >= dt.time(1,30,0)) & (data_ravenpack['time'] <= dt.time(7, 30, 0))]

data_ravenpack = data_ravenpack[['timestamp_utc', 'rp_entity_id', 'rp_story_id', 'relevance', 'ens', 'group']]
data_ravenpack = data_ravenpack.loc[data_ravenpack['relevance'] == 100]
data_ravenpack = data_ravenpack.loc[data_ravenpack['ens'] == 100]
data_ravenpack = data_ravenpack.loc[data_ravenpack['group'].isin(['acquisitions-mergers', 'analyst-ratings', 'assets', 'bankruptcy', 'credit', 'credit-ratings', 'dividends', 'earnings', 'equity-actions', 'labor-issues', 'products-services', 'revenues'])]
data_ravenpack['timestamp_utc'] = pd.to_datetime(data_ravenpack['timestamp_utc'])
data_ravenpack['timestamp'] = data_ravenpack.set_index('timestamp_utc').index.tz_localize('UTC').tz_convert('US/Eastern')
data_ravenpack.sort_values(['rp_entity_id','timestamp'], inplace=True)
data_ravenpack.set_index('timestamp', inplace=True)

data_taq['datetime'] = pd.to_datetime(data_taq['datetime'])
data_taq.sort_values(['rp_entity_id', 'datetime'], inplace=True)
data_taq.set_index('datetime', inplace=True)

# matching backward the nearest timestamp
data = pd.merge_asof(data_taq.sort_values(['datetime']),
                     data_ravenpack[['rp_entity_id', 'rp_story_id']].sort_values(['timestamp']),
                     left_index = True,
                     right_index = True,
                     by = 'rp_entity_id',
                     direction = 'backward',
                     allow_exact_matches = False)
# sorting and dropping duplicates
data.reset_index(0, inplace = True)
data.sort_values(['rp_entity_id', 'datetime'], inplace = True)
data.loc[data['rp_story_id'] == data['rp_story_id'].shift(), 'rp_story_id'] = np.nan
# dropping the obs whose price < 1
data = data.groupby(['symbol', 'date']).filter(lambda x: x['wvprice'].tail(1) > 1)
# data = data.loc[~ (data['wvprice'] < 1)]
# dropping the stocks that have no news
# data = data.groupby('symbol').filter(lambda x: x['rp_story_id'].count() > 0)
# mutate the ret_news and ret_nonnews columns
data['ret_nonnews'] = pd.np.where(data['rp_story_id'].isna(), data['adj_ret'], 0)
data['ret_news'] = pd.np.where(data['rp_story_id'].notna(), data['adj_ret'], 0)
data['ret_news'] = data['ret_news'] + 1
data['ret_nonnews'] = data['ret_nonnews'] + 1
# aggregating the data by day
data_retnews = data.groupby(['permno', 'date']).agg({'ret_news': 'prod'}).reset_index()
data_retnonnews = data.groupby(['permno', 'date']).agg({'ret_nonnews': 'prod'}).reset_index()
data_retnews['ret_news'] = data_retnews['ret_news'] - 1
data_retnonnews['ret_nonnews'] = data_retnonnews['ret_nonnews'] - 1
data.reset_index(0, inplace = True)

mask = data.groupby(['permno', 'date']).filter(lambda x: x['rp_story_id'].count() == 0)
mask = mask[['permno', 'date']].drop_duplicates(['permno', 'date'], keep = 'first')
df = data_retnews.merge(mask, on = ['permno', 'date'], how = 'inner')
data_retnews.loc[data_retnews.set_index(['permno', 'date']).index.isin(df.set_index(['permno', 'date']).index), 'ret_news'] = np.nan

data_retnews.to_csv('./data/output/data_retnews_2002.csv')
data_retnonnews.to_csv('./data/output/data_retnonnews_2002.csv')
data.to_csv('./data/output/data_news_2002.csv')
data_ravenpack.groupby('rp_entity_id').agg({'rp_story_id': 'nunique'}).to_csv('./data/news_count_2002.csv')
