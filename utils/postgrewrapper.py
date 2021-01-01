import pandas as pd
import psycopg2
import csv
import io
from sqlalchemy import create_engine, Integer, Float, String, DateTime, Text, BigInteger

class PostGreWrapper():
    '''
    Util Cllection of postgreSQL functionality
    '''
    def __init__(self,
                 params_dic = None):
        self.params_dic = params_dic
        self.rds_con = self.create_rds_con()

    def create_rds_con(self):
        '''
        Util Function to create and SQLEngine Instance
        '''
        return create_engine("postgresql://{un}:{pw}@{h}:{p}/{db}".format(
            un=self.params_dic['user'],
            pw=self.params_dic['password'],
            h=self.params_dic['host'],
            p=5432,
            db=self.params_dic['database']))


    def connect(self):
        '''
        Util function to connect to PostGreSql
        '''
        try:
            db_connection = psycopg2.connect(**self.params_dic)
        except (Exception, psycopg2.DatabaseError) as error:
            raise (error)
        self.db_connection = db_connection
        return db_connection


    def copy_to_rds(self, tables_dict):
        '''
        fast Wrapper to put the data into SQL in a Faster Manner
        '''
        if not hasattr(PostGreWrapper, 'db_connection'):
            self.connect()
        conn = self.db_connection
        cursor = conn.cursor()

        try:
            for table_name, df in tables_dict.items():
                # dictionary to coerce datatype in
                cursor.execute(
                    f"""SELECT EXISTS
                        (SELECT FROM pg_tables 
                        WHERE schemaname = 'real_time' 
                            AND tablename  = '{table_name.split('.')[1]}')""")
                IfTableExist = cursor.fetchone()[0]
                if IfTableExist:
                    pass
                else:
                    print("Create New table")
                    data = pd.DataFrame(df.dtypes)
                    data = data.to_dict()[0]
                    for key, value in data.items():
                        if str(value) == "int64":
                            data[key] = BigInteger()
                        elif str(value) == "float64":
                            data[key] = Float()
                        # Handle DateTime
                        # elif str(value) == "float64":
                        #     data[key] = Float()
                        else:
                            data[key] = String()
                    df[:0].to_sql(table_name.split(".")[1], self.rds_con, schema=table_name.split(
                        ".")[0], index=False, if_exists='append', chunksize=500, dtype=data)
                    print(f"Table {table_name} Added")
                output = io.StringIO()
                df.to_csv(output, sep='|', escapechar='\\',
                        header=True, index=False,
                        line_terminator = "\n",quoting = csv.QUOTE_NONNUMERIC)
                output.seek(0)
                sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS '|'"
                cursor.copy_expert(sql, output)
                del output
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            conn.rollback()
            cursor.close()
            conn.close()
            raise ValueError('Could not update RDS')

        cursor.close()
        conn.close()
        return True