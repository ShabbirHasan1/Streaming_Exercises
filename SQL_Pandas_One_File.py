import mysql.connector
from pandas import DataFrame
from ibapi.client import EClient
from ibapi.common import *
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from DBHelperMay import DBHelper

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
        df = self.getDataInPandaDF()
        print(df)

def main():

    app = GooseIndicator()
    try:

        app.get_SQL_into_pandas()

    except:
        raise


if __name__ == "__main__":
    main()