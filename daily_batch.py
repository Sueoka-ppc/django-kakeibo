import sys,os
import psycopg2
import datetime 
import pandas as pd 
from dotenv import load_dotenv

load_dotenv(verbose=True)

class Daily_Batch(object):
    def __init__(self):
        self.DSN = os.environ.get("DB_NAME")
        self.USN = os.environ.get("USER_NAME")
        self.PWD = os.environ.get("PASSWORD")
        self.HOST = os.environ.get("HOST_NAME")
        self.PORT = os.environ.get("PORT")


    def data_trans_first_tb(self):

        try:
            conn = psycopg2.connect("postgresql://{username}:{password}@{hostname}:{port}/{dbName}".format(
                username = self.USN,
                password = self.PWD,
                hostname = self.HOST,
                port = self.PORT,
                dbName = self.DSN
            ))

            cur = conn.cursor()
        except Exception as e:
            print(e)
        
        select_sql="SELECT * FROM kakeibo_payment WHERE kouza_id = 1"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza1_payment"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza1_payment(id, date, price, description, category_id, paycategory_id)VALUES (%s, %s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        select_sql="SELECT * FROM kakeibo_income WHERE kouza_id = 1"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza1_income"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza1_income(id, date, price, description, category_id)VALUES (%s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        conn.commit()

        cur.close()
        conn.close()



    def data_trans_second_tb(self):

        try:
            conn = psycopg2.connect("postgresql://{username}:{password}@{hostname}:{port}/{dbName}".format(
                username = self.USN,
                password = self.PWD,
                hostname = self.HOST,
                port = self.PORT,
                dbName = self.DSN
            ))

            cur = conn.cursor()
        except Exception as e:
            print(e)
        
        select_sql="SELECT * FROM kakeibo_payment WHERE kouza_id = 2"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza2_payment"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza2_payment(id, date, price, description, category_id, paycategory_id)VALUES (%s, %s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        select_sql="SELECT * FROM kakeibo_income WHERE kouza_id = 2"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza2_income"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza2_income(id, date, price, description, category_id)VALUES (%s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)
        
        conn.commit()
        cur.close()
        conn.close()

    def data_trans_third_tb(self):

        try:
            conn = psycopg2.connect("postgresql://{username}:{password}@{hostname}:{port}/{dbName}".format(
                username = self.USN,
                password = self.PWD,
                hostname = self.HOST,
                port = self.PORT,
                dbName = self.DSN
            ))

            cur = conn.cursor()
        except Exception as e:
            print(e)
        
        select_sql="SELECT * FROM kakeibo_payment WHERE kouza_id = 3"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza3_payment"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza3_payment(id, date, price, description, category_id, paycategory_id)VALUES (%s, %s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        select_sql="SELECT * FROM kakeibo_income WHERE kouza_id = 3"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza3_income"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza3_income(id, date, price, description, category_id)VALUES (%s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        conn.commit()
                
        cur.close()
        conn.close()

    def data_trans_forth_tb(self):

        try:
            conn = psycopg2.connect("postgresql://{username}:{password}@{hostname}:{port}/{dbName}".format(
                username = self.USN,
                password = self.PWD,
                hostname = self.HOST,
                port = self.PORT,
                dbName = self.DSN
            ))

            cur = conn.cursor()
        except Exception as e:
            print(e)
        
        select_sql="SELECT * FROM kakeibo_payment WHERE kouza_id = 4"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza4_payment"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza4_payment(id, date, price, description, category_id, paycategory_id)VALUES (%s, %s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        select_sql="SELECT * FROM kakeibo_income WHERE kouza_id = 4"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza4_income"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza4_income(id, date, price, description, category_id)VALUES (%s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        conn.commit()                
        cur.close()
        conn.close()


    def data_trans_fifth_tb(self):

        try:
            conn = psycopg2.connect("postgresql://{username}:{password}@{hostname}:{port}/{dbName}".format(
                username = self.USN,
                password = self.PWD,
                hostname = self.HOST,
                port = self.PORT,
                dbName = self.DSN
            ))

            cur = conn.cursor()
        except Exception as e:
            print(e)
        
        select_sql="SELECT * FROM kakeibo_payment WHERE kouza_id = 5"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza5_payment"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza5_payment(id, date, price, description, category_id, paycategory_id)VALUES (%s, %s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        select_sql="SELECT * FROM kakeibo_income WHERE kouza_id = 5"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza5_income"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza5_income(id, date, price, description, category_id)VALUES (%s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        conn.commit()        
        cur.close()
        conn.close()

    """def data_trans_sixth_tb(self):

        try:
            conn = psycopg2.connect("postgresql://{username}:{password}@{hostname}:{port}/{dbName}".format(
                username = self.USN,
                password = self.PWD,
                hostname = self.HOST,
                port = self.PORT,
                dbName = self.DSN
            ))

            cur = conn.cursor()
        except Exception as e:
            print(e)
        
        select_sql="SELECT * FROM kakeibo_payment WHERE kouza_id = 6"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza5_payment"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza6_payment(id, date, price, description, category_id, paycategory_id)VALUES (%s, %s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)

        select_sql="SELECT * FROM kakeibo_income WHERE kouza_id = 6"
        select_data = pd.read_sql(sql=select_sql,con=conn)
        select_data = select_data.drop(columns='kouza_id')
        print(select_data)
        select_tuple =  [tuple(x) for x in select_data.values]

        print(select_tuple)

        delete_sql = "DELETE from monthly_kouza6_income"

        cur.execute(delete_sql)

        insert_sql = "INSERT INTO public.monthly_kouza6_income(id, date, price, description, category_id)VALUES (%s, %s, %s, %s, %s);"
        cur.executemany(insert_sql,select_tuple)
                
        cur.close()
        conn.close()
    """


if __name__ == "__main__":
    bdn = Daily_Batch()
    bdn.data_trans_first_tb()
    bdn.data_trans_second_tb()
    bdn.data_trans_third_tb()
    bdn.data_trans_forth_tb()
    bdn.data_trans_fifth_tb()
