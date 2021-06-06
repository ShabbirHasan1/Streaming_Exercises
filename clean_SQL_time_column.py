

import pandas as pd


from datetime import datetime

# https://stackoverflow.com/questions/3682748/converting-unix-timestamp-string-to-readable-date

df1 = pd.read_csv('SQL_Streaming.csv')

#print(df1)

clean_df = pd.DataFrame()
clean_df['date'] = df1['time']
# print(clean_df)

# https://github.com/ebtrader/goose_algo/blob/master/Goose_WTDEHMA.py
# df['converted_time'] = pd.to_datetime(df['time'], unit = 's') - pd.Timedelta(4, unit = 'h')
# https://stackoverflow.com/questions/19231871/convert-unix-time-to-readable-date-in-pandas-dataframe
clean_df['date'] = pd.to_datetime(clean_df['date'], unit = 's') - pd.Timedelta(4, unit = 'h')
clean_df['close'] = df1['price']

print(clean_df)