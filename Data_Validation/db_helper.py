
import sqlalchemy as db
import pandas as pd
import json



class DbHelper:
    def __init__(self):
        pass

    def create_conn(self, conn_string):
        """
               Create connection with database.
               :param conn_string: Connection String required to build connection.
        """
        try:
            self.database_engine = db.create_engine(conn_string)
            self.db_connection = self.database_engine.connect()
            self.db_metadata = db.MetaData()
        except Exception as e:
            print(e)

    def query_execution(self, query):
        """
            Execute query on database.
            :param query: Required Query.
        """
        try:
            df = pd.read_sql_query(query, self.db_connection)
            return df
        except Exception as e:
            print(e)

    def generate_connection_string(self, database, username, passw, host, port, dbname, sqlite_db_path):
        """
            Generate a connection string to connect with database.
            :param database: Database (mysql, postgresql, oracle).
            :param username: Db username
            :param  passw: Db Password
            :param  host: Db host
            :param  port: Port # required
            :param  dbname: Database Name/ Service Name (ORACLE)
            :param sqlite_db_path: Path for sqlite db file.
        """
        conn_string = ""
        if database == "mysql":
            conn_string = database+"+pymysql://"+username+":"+passw+"@"+host+":"+port+"/"+dbname
        elif database == "postgresql":
            conn_string = database + "+psycopg2://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "oracle":
            conn_string = database + "+cx_oracle://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "mariadb":
            conn_string = database + "+mariadb://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "mssql":
            conn_string = database + "+pymssql://" + username + ":" + passw + "@" + host + ":" + port + "/" + dbname
        elif database == "sqlite":
            conn_string = "'"+database+"'///"+sqlite_db_path
        return conn_string

    def close_conn(self):
        try:
            self.db_connection.close()
        except Exception as e:
            print(e)

    def query_builder(self, file_name, query_name):
        """
             Generate query on database.
             :param file_name: Required Query.
         """
        try:
            with open(file_name) as f:
                data = json.load(f)

            for query in data:
                if query == query_name:
                    base_query = 'select '
                    if bool(data[query]['select']):
                        for cols in data[query]['select']:
                            if data[query]['select'][cols]['function'] != "":
                                base_query += (data[query]['select'][cols]['function']) + "(" + cols + ")" + ","
                            else:
                                base_query += cols + ","
                        base_query = base_query[:-1]
                    else:
                        base_query += "*"
                    if bool(data[query]['database']):
                        base_query = base_query + " from " + data[query]['database'] + "." + data[query]['table']
                    else:
                        base_query = base_query + " from " + data[query]['table']
                    if bool(data[query]['where']):
                        testlist = list(data[query]['where'])
                        base_query = base_query + " where"
                        for index, key in enumerate(data[query]['where']):
                            base_query += " " + key + data[query]['where'][key]['operator']
                            if bool(data[query]['where'][key]['operation']):
                                base_query += data[query]['where'][key]['value'] + " " + data[query]['where'][key]['operation']
                                if index + 1 < len(testlist):
                                    pass
                                else:
                                    length = len(data[query]['where'][key]['operation'])
                                    base_query = base_query[:(-1) * (length+1)]

                            else:
                                base_query += data[query]['where'][key]['value']
                                break

                return base_query
        except Exception as e:
            print(e)
