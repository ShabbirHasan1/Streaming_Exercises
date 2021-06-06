import os.path
import mysql.connector
import pandas as pd
from pandas import DataFrame
from ibapi.client import EClient
from ibapi.common import *
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from datetime import datetime

class GooseIndicator(EWrapper, EClient):
    def __init__(self):
        super().__init__()
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

    def getDBConnection(self):

        try:
            connection = mysql.connector.connect(host='localhost',
                                                 database='nqdatabase',
                                                 user='root',
                                                 password='suite203',
                                                 auth_plugin='mysql_native_password')

            #print("Connection Established with DB")
            return connection

        except mysql.connector.Error as error:
            print("Failed to connect to DB {}".format(error))
            if (connection.is_connected()):
                connection.close()
                print("MySQL connection is closed")

    def getDataInPandaDF(self):

        try:
            connection = self.getDBConnection()
            sql_select_query = """SELECT * FROM tick_by_tick_all_last """

            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_select_query)

            df = DataFrame(cursor.fetchall())
            df.columns = cursor.column_names
            print(df.head())

            return df

        except mysql.connector.Error as error:
            print("Failed to select record from tick_by_tick_all_last table {}".format(error))

        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()


    def get_SQL_into_pandas(self):
        # https: // stackoverflow.com / questions / 54944524 / how - to - write - csv - file - into - specific - folder
        # file_path = f"data/ticks_{contract.symbol}_{today}.csv"
        now = datetime.now()
        today = now.strftime("%m%d%Y_%H_%M_%S")

        df = self.getDataInPandaDF()
        df['date'] = pd.to_datetime(df['time'], unit='s') - pd.Timedelta(4, unit='h')
        # print(df)
        # df.to_csv('clean_SQL.csv')

        # mask = (df['date'] > '2021-05-28') & (df['date'] <= '2000-6-10')
        # https://stackoverflow.com/questions/29370057/select-dataframe-rows-between-two-dates
        date_after = '2021-06-01'
        mask = df['date'] > date_after
        df1 = df.loc[mask]
        print(df1)

        df1.to_csv(f'{today}.csv')

def main():

    app = GooseIndicator()
    try:

        app.get_SQL_into_pandas()

    except:
        raise


if __name__ == "__main__":
    main()