# %%
import pandas as pd
from zhconv import convert
from scipy import stats
from fuzzywuzzy import fuzz
from time import perf_counter
import searchconsole
from datetime import datetime
from datetime import timedelta

# --------------DATA RETRIVING---------------
# no credentials saved, do not save credentials
#account = searchconsole.authenticate(client_config='client_secrets.json')

# no credentials saved, want to save credentials
#account = searchconsole.authenticate(client_config='client_secrets.json', serialize = 'credentials.json')

# credentials saved as credentials.json
account = searchconsole.authenticate(client_config='client_secrets.json',
                                     credentials='credentials.json')

# webproperty must match what's shown on Google Search Console
webproperty = account['******'] # website url

start = datetime.strptime("******", "%Y-%m-%d") # modify start date
end = datetime.strptime("******", "%Y-%m-%d") # modify end date

df = pd.DataFrame()

while start != end:
    start_datetime = datetime.strftime(
        start, "%Y-%m-%d")
    # interval = 1 day
    shifted_datetime = datetime.strftime(start + timedelta(days=1), "%Y-%m-%d")

    report = webproperty.query.range(
        start_datetime, shifted_datetime).dimension("query").get()

    df1 = pd.DataFrame(report.rows)
    df1['date'] = datetime.strftime(start, "%Y-%m-%d")
    df = pd.concat([df, df1])
    print(f"Trend of {start} retrived")
    start = start + timedelta(days=1)

print("=========================")
print("ALL DATA RETRIVED")
print("=========================")
df

# df.to_csv('trend.csv', index=False)

# --------------DATA RETRIVING FINISHED---------------

# %%
# --------------DATA PREPARATION---------------
# unify characters, merge similar keywords
# Merge split words and convert Tranditional Chinese to Simplified Chinese -> 'modified_query'
df['modified_query'] = df['query'].apply(lambda x: convert(x, 'zh-cn'))
df['modified_query'] = df['modified_query'].apply(lambda x: x.replace(" ", ""))

# Identify similar keywords
# option 1: fuzzy match words
# TODO: use process in stead of iteration: 
# https://towardsdatascience.com/fuzzywuzzy-find-similar-strings-within-one-column-in-a-pandas-data-frame-99f6c2a0c212
# http://jonathansoma.com/lede/algorithms-2017/classes/fuzziness-matplotlib/fuzzing-matching-in-pandas-with-fuzzywuzzy/
similar = df[['modified_query', 'query']
             ].drop_duplicates(subset='modified_query')
similar1 = similar

# record time
timer1 = perf_counter()

for index, row in similar1.iterrows():  # for each row in second df
    for index1, row1 in similar1.iterrows():  # loop through the whole second df
        r = fuzz.token_sort_ratio(row['modified_query'],
                                  row1['modified_query'])
        if r > 80:  # 80 is for conservitive result
            # if match, add it into main df
            similar.loc[index1, 'aggregated_query'] = row['modified_query']
            # and drop the row in second df
            similar1 = similar1.drop(index1, axis=0)
    print(f"{len(similar1)} rows remain")
print("=========================")
print(
    f"{len(similar['aggregated_query'].unique())} unique keywords identified")
print("=========================")
timer2 = perf_counter()
print(f"Identifying Keywords: {timer2 - timer1} Seconds")

# put identified keywords back to df
df = pd.merge(df, similar, how='left', on='modified_query')

# record time
timer3 = perf_counter()
print(f"Total running time: {timer3 - timer1} Seconds")

df.to_csv('prepared.csv')
df

# option 2: count words frequency


# --------------DATA PREPARATION FINISHED---------------

# %%
# --------------LEFT PATH---------------
# sum clicks and impressions for each keywords, to find the top keywords that worth analyzing

# extract columns and sum
total_count = df[['aggregated_query', 'clicks', 'impressions']]
total_count = total_count.groupby('aggregated_query').sum().reset_index()

# pick fixed number of rows
# TODO: switch to dynamic picking
top_clicks = total_count.nlargest(
    int(round(len(total_count) * 0.2)), ['clicks'])
top_impressions = total_count.nlargest(
    int(round(len(total_count) * 0.2)), ['impressions'])

# test df
#hc_hi = pd.merge(top_clicks, top_impressions, how='inner', on=['modified_query'])[['modified_query', 'clicks_x', 'impressions_x']].rename(columns={'clicks_x': 'clicks', 'impressions_x': 'impressions'})

# categorization
hc_hi = top_clicks['aggregated_query'][top_clicks['aggregated_query'].isin(
    top_impressions['aggregated_query'])]  # rows in both top results
hc_li = top_clicks['aggregated_query'][~top_clicks['aggregated_query'].isin(
    top_impressions['aggregated_query'])]  # rows in clicks top results but not impressions top results
lc_hi = top_impressions['aggregated_query'][~top_impressions['aggregated_query'].isin(
    top_clicks['aggregated_query'])]  # rows in impressions top results but not clicks top results

total_count.loc[total_count['aggregated_query'].isin(
    hc_hi), 'category'] = 'hc_hi'
total_count.loc[total_count['aggregated_query'].isin(
    hc_li), 'category'] = 'hc_li'
total_count.loc[total_count['aggregated_query'].isin(
    lc_hi), 'category'] = 'lc_hi'

category_df = total_count.dropna(subset=['category'])
category_df

#%%
#TODO:
# split keywords, drop duplicated, and get Google Trends







# --------------LEFT PATH END---------------


# %%
# --------------MIDDLE PATH---------------
# Aggregate modified query on the same day
merged_df = df[['aggregated_query', 'clicks', 'impressions', 'date']]

# identify keywords repeated in the same day and sum them
duplicated = merged_df[merged_df.duplicated(
    ['aggregated_query', 'date'], keep=False)]
duplicated = duplicated.groupby(['aggregated_query', 'date']).sum()[
    ['clicks', 'impressions']].reset_index()

# concat with non-repeat keywords
not_duplicated = merged_df.drop_duplicates(
    ['aggregated_query', 'date'], keep=False)
merged_df = pd.concat([duplicated, not_duplicated])

# recalculate CTR
merged_df['CTR'] = merged_df['clicks'] / merged_df['impressions']

merged_df

# %%
# Filter merged_df by category list
filtered_df = pd.merge(merged_df, category_df, how='inner', on=['aggregated_query'])[
    ['aggregated_query', 'date', 'clicks_x', 'impressions_x', 'CTR', 'category']].rename(columns={'clicks_x': 'clicks', 'impressions_x': 'impressions'})

# %%
# simple linear regression
# test not to group
slope_df = filtered_df

slope_df['date'] = pd.to_datetime(
    slope_df['date']).map(datetime.toordinal)

# clicks and impressions both uptrend = uptrend
# mixed = sideways
# both downtrend = downtrend

# clicks linear regression
clicks = pd.DataFrame(
    columns=['aggregated_query', 'slope', 'intercept', 'r_value', 'p_value', 'std_err'])
for query in slope_df['aggregated_query'].unique():
    df_query = slope_df[slope_df['aggregated_query'] == query]
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        df_query['date'], df_query['clicks'])
    clicks = clicks.append({"aggregated_query": query, "slope": slope, "intercept": intercept,
                                      "r_value": r_value, "p_value": p_value, "std_err": std_err}, ignore_index=True)
clicks = clicks[clicks['p_value'] < 0.1]
clicks.sort_values(
    'slope', ascending=False, inplace=True)

# impressions linear regression
impressions = pd.DataFrame(
    columns=['aggregated_query', 'slope', 'intercept', 'r_value', 'p_value', 'std_err'])
for query in slope_df['aggregated_query'].unique():
    df_query = slope_df[slope_df['aggregated_query'] == query]
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        df_query['date'], df_query['impressions'])
    impressions = impressions.append({"aggregated_query": query, "slope": slope, "intercept": intercept,
                                                "r_value": r_value, "p_value": p_value, "std_err": std_err}, ignore_index=True)
impressions = impressions[impressions['p_value'] < 0.1]
impressions.sort_values(
    'slope', ascending=False, inplace=True)

# merge and identify trend
slope = pd.merge(clicks, impressions, how='outer', on='aggregated_query')
for index, row in slope.iterrows():
    if row['slope_x'] > 0 and row['slope_y'] > 0:
        slope.loc[index, 'trend'] = 'uptrend'
    elif row['slope_x'] < 0 and row['slope_y'] < 0:
        slope.loc[index, 'trend'] = 'downtrend'
    else:
        slope.loc[index, 'trend'] = 'sideways trend' # including clicks or impressions p value > required value

slope

"""
CTR version instead of clicks
# CTR and impressions both uptrend = uptrend
# mixed = sideways
# both downtrend = downtrend

# CTR linear regression
ctr = pd.DataFrame(
    columns=['aggregated_query', 'slope', 'intercept', 'r_value', 'p_value', 'std_err'])
for query in slope_df['aggregated_query'].unique():
    df_query = slope_df[slope_df['aggregated_query'] == query]
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        df_query['date'], df_query['CTR'])
    ctr = ctr.append({"aggregated_query": query, "slope": slope, "intercept": intercept,
                                      "r_value": r_value, "p_value": p_value, "std_err": std_err}, ignore_index=True)
ctr = ctr[ctr['p_value'] < 0.1]
ctr.sort_values(
    'slope', ascending=False, inplace=True)

# impressions linear regression
impressions = pd.DataFrame(
    columns=['aggregated_query', 'slope', 'intercept', 'r_value', 'p_value', 'std_err'])
for query in slope_df['aggregated_query'].unique():
    df_query = slope_df[slope_df['aggregated_query'] == query]
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        df_query['date'], df_query['impressions'])
    impressions = impressions.append({"aggregated_query": query, "slope": slope, "intercept": intercept,
                                                "r_value": r_value, "p_value": p_value, "std_err": std_err}, ignore_index=True)
impressions = impressions[impressions['p_value'] < 0.1]
impressions.sort_values(
    'slope', ascending=False, inplace=True)

# merge and identify trend
slope = pd.merge(ctr, impressions, how='outer', on='aggregated_query')
for index, row in slope.iterrows():
    if row['slope_x'] > 0 and row['slope_y'] > 0:
        slope.loc[index, 'trend'] = 'uptrend'
    elif row['slope_x'] < 0 and row['slope_y'] < 0:
        slope.loc[index, 'trend'] = 'downtrend'
    else:
        slope.loc[index, 'trend'] = 'sideways trend' # including clicks or impressions p value > required value

slope
"""

#%%
# TODO: Google Trends cross-reference



# FINAL OUTPUT 1:
slope


# --------------MIDDLE PATH---------------


# %%
# --------------RIGHT PATH---------------
# create a lookup table for qoriginal and modified queries
query_lockup = df[['aggregated_query', 'modified_query', 'query_x']].drop_duplicates(
    ['aggregated_query', 'modified_query', 'query_x']).sort_values(['aggregated_query', 'modified_query']).reset_index(drop=True)

# # pivot
# query_lockup['count'] = None
# for index, row in query_lockup.iterrows():
#     if index == 0:
#         row['count'] = 1
#     elif row['aggregated_query'] != query_lockup.loc[index - 1, 'aggregated_query']:
#         row['count'] = 1
#     elif row['aggregated_query'] == query_lockup.loc[index - 1, 'aggregated_query']:
#         row['count'] = query_lockup.loc[index - 1, 'count'] + 1
# query_lockup['count'].astype(int)

# query_df = query_lockup.pivot(
#     index='aggregated_query', columns='count', values='query_x').reset_index()
# query_df.columns.name = None

query_lockup.columns = ['aggregated_query', 'modified_query', 'original_query']

# filter keywords with identified trends
trend = pd.merge(query_lockup, slope, how='left', on='aggregated_query').dropna(
    subset=['trend'])[['aggregated_query', 'original_query', 'trend']].reset_index(drop=True)

#%%
# get average positions from Google Search Console
report = webproperty.query.range(
    start, end).dimension("query").get()

position = pd.DataFrame(report.rows)[['query', 'position']]

query_trend_output = trend.merge(position, how='left', left_on='original_query', right_on='query')

# FINAL OUTPUT 2:
query_trend_output

# --------------RIGHT PATH---------------

