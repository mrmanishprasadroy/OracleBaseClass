from DataManager.DbManager import OracleDB


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def start_app():
    # create an instance of the class and connect to the database
    oracle = OracleDB(username='SYM', password='SYM', host='10.182.50.56', port='1521',
                      service_name='WMSPROD')
    oracle.connect()
    if oracle.state:
        df = oracle.fetch_telegram_data('CRANE_WMS_OCCUPANCY')
        print(df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_app()
