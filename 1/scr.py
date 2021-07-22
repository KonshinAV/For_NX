import os
import fdb
import pyodbc
import sys

class FDBServer:
    def __init__(self, fdb_destination, fdb_user, fdb_password):
        self.fdb_destination = fdb_destination
        self.fdb_user = fdb_user
        self.fdb_password = fdb_password
        self.fdb_con = None
        self.fdb_cursor = None
        self.connect()

    def connect(self):
        try:
            self.fdb_con = fdb.connect(dsn=self.fdb_destination, user=self.fdb_user, password=self.fdb_password)
            self.fdb_cursor = self.fdb_con.cursor()
        except fdb.fbcore.DatabaseError as ex:
            print (f"[-]: Не удалось подключиться к БД Firebird {self.fdb_destination} \nРабота приложения завершена")
            sys.exit()

        pass

    def get_table_data(self, fdb_table):
        try:
            self.fdb_cursor.execute(f"SELECT * FROM {fdb_table}")
            print(f"[+] Загружены данные для {fdb_table}, Firebird")
            return self.fdb_cursor.fetchall()
        except fdb.fbcore.DatabaseError:
            print(f"[-] Не удалось выполнить запрос для {fdb_table}")
            return False
        pass

class MsSqlServer:
    def __init__(self, server, database, username, pwd, autoconnect = True, driver = '{SQL Server}', trusted_connection = 'yes'):
        self.server = server
        self.database = database
        self.username = username
        self.pwd = pwd
        self.driver = driver
        self.cursor = None
        self.conn = None
        self.trusted_connection = trusted_connection
        if autoconnect: self.connect()

    def connect(self):
        self.conn = pyodbc.connect(f"DRIVER={self.driver};SERVER={self.server};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.pwd};Trusted_Connection={self.trusted_connection}")
        self.cursor = self.conn.cursor()
        pass

    def get_table_data(self, table):
        try:
            self.cursor.execute (f'''
                                SELECT * FROM {table}
                                ''')
            return self.cursor.fetchall()
        except pyodbc.ProgrammingError as ex:
            print(f'[-] При обработке возникла ошибка {ex}')
            return False

    def write_data_to_table (self, table, columns, values):
        request = f"INSERT INTO {table} {columns} VALUES {values}"
        self.cursor.execute(request)
        self.conn.commit()
        pass

    def check_exist_data_in_table (self, sur_name, name, table):
        self.cursor.execute(f"""
                            SELECT 1 FROM {table} WHERE SURNAME = '{sur_name}' AND NAME = '{name}'  
                            """)
        return False if len(self.cursor.fetchall()) == 0 else True

class TxtFile:
    def __init__(self, path, default_read=True):
        self.path = path
        self.data = None
        if default_read: self.data = self.read_data()

    def already_exists(self):
        return True if os.path.exists(self.path) else False

    def write_report(self, data):
        with open(self.path, 'w')  as f:
            for i in data: f.write(str(i).replace('(','').replace(')','').replace("'", '')+'\n')
            return True

    def read_data(self):
        try:
            with open(self.path, 'r') as f:
                self.data = f.read().splitlines()
                print (f'[+] Файл {self.path} прочитан')
                return self.data
        except FileNotFoundError as ex:
            print(f"[+] Файл <{self.path}> не обнаружен")
            return False

def main ():
    fdb_serv = FDBServer(fdb_destination=f'127.0.0.1:{os.getcwd()}/DB_FIREBIRD.FDB',fdb_user='SYSDBA',fdb_password='masterkey')
    fdb_serv.connect()
    print(f"[+] Подключение к БД Firefird {fdb_serv.fdb_destination}")
    u_names_fdb = fdb_serv.get_table_data('USERS_NAMES')
    sur_names_fdb = fdb_serv.get_table_data('USERS_SURNAMES')
    print (f"[+] загрузка данных из БД Firebird")

    ms_sql_server = MsSqlServer(server='localhost\SQLEXPRESS',
                                     database='Test',
                                     username='ms_sql_user',
                                     pwd='ms_sql_user',
                                     trusted_connection='no', )
    ms_sql_server.connect()
    print (f"[+] Подключение к MsSQL {ms_sql_server.server}\\{ms_sql_server.database}")

    print(f"[+] Проверка наличия файлов отчета")
    u_names_txt = TxtFile('rep_USERS_NAMES.txt')
    sur_names_txt = TxtFile('rep_USERS_SURNAMES.txt')
    writing_txt_reports = (u_names_txt.write_report(u_names_fdb) if not u_names_txt.already_exists() else print (f'[+] Файл {u_names_txt.path} существует'),
                           sur_names_txt.write_report(sur_names_fdb) if not sur_names_txt.already_exists() else print (f'[+] Файл {sur_names_txt.path} существует'))

    if writing_txt_reports[0] is None or writing_txt_reports[1] is None:
        print(f'''Работа с приложением будет завершена, обнаружены файлы с существующими именами''')
        sys.exit()
    else:
        records_count = min(len(u_names_fdb), len(sur_names_fdb))
        # if len(u_names_fdb) == len(sur_names_fdb):
        u_names_txt.read_data()
        sur_names_txt.read_data()
        query = ''
        ind = 0
        while ind != records_count:
            i_surname = sur_names_txt.data[ind].split(', ')[1]
            i_name = u_names_txt.data[ind].split(',')[1]
            # print (f"ind {ind}, surmane {i_surname}, name {i_name}")
            if ms_sql_server.check_exist_data_in_table(i_surname, i_name, 'Test.dbo.Table_1') is False:
                query = query + (f"('{i_surname}', '{i_name}'),")
            # print (f"{i_surname} and {i_name} Exist in table = {ms_sql_server.check_exist_data_in_table(i_surname, i_name, 'Test.dbo.Table_1')}")
            ind = ind+1

        print(f"[+] Выгрузка сформирована")
        # print (f"query = {query}")
        if len(query) != 0:
            ms_sql_server.write_data_to_table(table='Test.dbo.Table_1',columns='(SURNAME, NAME)',values=f"{query[0:-1]}")
        else:
            print('[-] Пустой запрос, нет уникальных данных для записи')
        print(f"[+] Выгрузка данных на сервер {ms_sql_server.server}\\{ms_sql_server.database} завершена")

if __name__ == '__main__':
    main()
    pass