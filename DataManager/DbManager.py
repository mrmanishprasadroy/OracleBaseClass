from collections import defaultdict
from xml.etree.ElementTree import ElementTree
import cx_Oracle
import pandas as pd


class OracleDB:
    def __init__(self, username, password, host, port, service_name):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.service_name = service_name
        self.connection = None
        self.cursor = None
        self.state = False

    def connect(self):
        try:
            dsn = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
            self.connection = cx_Oracle.connect(user=self.username, password=self.password, dsn=dsn)
            self.cursor = self.connection.cursor()
            self.state = True
        except cx_Oracle.DatabaseError as e:
            error = e.args
            print(f"Error code: {error.code}")
            print(f"Error message: {error.message}")
            print("There was a problem connecting to the database")
            self.state = False

    def close(self):
        self.cursor.close()
        self.connection.close()
        self.state = False

    def query_df(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        cols = [i[0] for i in self.cursor.description]
        return pd.DataFrame(rows, columns=cols)

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def result_count(self, sql):
        self.cursor.execute(sql)
        return len(self.cursor.fetchall())

    @staticmethod
    def process_data(row):
        xml_str = str(row['DATA'].replace("_x000D_", ""))
        root = ElementTree.fromstring(xml_str.lstrip())
        data = defaultdict(str)
        for elem in root.iter():
            if len(elem) == 0:
                data[elem.tag] = elem.text
        data["TimeStamp"] = row['MODIF_LAST']
        return data

    def fetch_telegram_data(self, name: str) -> pd.DataFrame:
        try:
            select_str = "select * from tcp_tel_hist where telegram_name = '{0}';".format(name)
            df = self.query_df(select_str)
            df_data = pd.DataFrame(df, columns=['MODIF_LAST', 'DATA'])
            df_data['data_dict'] = df_data.apply(self.process_data, axis=1)
            data = df_data['data_dict'].to_dict()
            new_df = pd.DataFrame(data).T
            new_df["TimeStamp"] = pd.to_datetime(new_df["TimeStamp"])
            return new_df
        except Exception as e:
            print("An error occurred:", e)
            return pd.DataFrame()
