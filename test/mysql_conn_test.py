from math import ceil
import pymysql
import pandas as pd


class MySQLTool:

    def __init__(self, batch_size=10000):
        self.batch_size = 1000
        self.db = None

    def connect(
            self,
            host,
            user,
            passwd,
            db,
            cursorclass=pymysql.cursors.DictCursor
    ):
        self.db = pymysql.connect(
            host=host,
            user=user,
            passwd=passwd,
            db=db,
            cursorclass=cursorclass
        )

    def ali_connect(self):
        self.db = pymysql.connect(
            host="rm-uf6p57w6z8imq8h0pgo.mysql.rds.aliyuncs.com",
            user="ctripdi_prodb",
            passwd="ctripdi_71377",
            db="testdb",
            cursorclass=pymysql.cursors.DictCursor
        )

    def close(self):
        self.db.close()

    def reset_db(self):
        self.db = None

    def select(
            self,
            sql,
            # batch_size=10000
    ):
        cursor = self.db.cursor()
        ttl_row = cursor.execute(sql)
        print('total rows: ' + str(ttl_row))

        assert isinstance(self.batch_size, int)

        if self.batch_size == -1:
            res_list = cursor.fetchall()
        else:
            print('batch size: ' + str(self.batch_size))
            res_list = []
            batch_num = ceil(ttl_row / self.batch_size)
            for b_i in range(batch_num):
                print('batch: ' + str(b_i))
                res_list += cursor.fetchmany(self.batch_size)
        res_df = pd.DataFrame(res_list)
        cursor.close()
        return res_df

    def create_table(
            self,
            sql
    ):
        cursor = self.db.cursor()
        ttl_row = cursor.execute(sql)
        print('total rows: ' + str(ttl_row))
        self.db.commit()
        cursor.close()

    def insert(
            self,
            tbl_name,
            df,
            col_list=None
    ):

        row_num = df.shape[0]
        print('row num to insert: ' + str(row_num))
        batch_num = ceil(row_num / self.batch_size)

        cursor = self.db.cursor()
        if not col_list:
            col_list = list(df.columns)
        sql_insert = """
        replace into %s (""" + ','.join(['%s'] * len(col_list)) + """) values (""" + ','.join(['%%(%s)s'] * len(col_list)) + ')'
        sql_insert = sql_insert % tuple([tbl_name] + col_list + col_list)

        df_dict_list = df.to_dict(orient='records')
        for b_i in range(batch_num):
            cursor.executemany(
                sql_insert,
                df_dict_list[b_i * self.batch_size: (b_i + 1) * self.batch_size]
            )
            self.db.commit()
        cursor.close()

    def update(
            self,
            tbl_name,
            df,
            col_list=None,
            conditions=None
    ):

        row_num = df.shape[0]
        print('row num to update: ' + str(row_num))
        batch_num = ceil(row_num / self.batch_size)

        cursor = self.db.cursor()
        conditions = """""" if not conditions else conditions

        if not col_list:
            col_list = list(df.columns)
        col_list_double = []
        for col in col_list:
            col_list_double.append(col)
            col_list_double.append(col)

        sql_update = """
            update %s set """ + ','.join(['%s = %%(%s)s'] * len(col_list)) + ' ' + conditions
        sql_update = sql_update % tuple([tbl_name] + col_list_double)

        df_dict_list = df.to_dict(orient='records')
        for b_i in range(batch_num):
            cursor.executemany(
                sql_update,
                df_dict_list[b_i * self.batch_size: (b_i + 1) * self.batch_size]
            )
            self.db.commit()
        cursor.close()


if __name__ == '__main__':

    futu_sql = MySQLTool()
    futu_sql.ali_connect()
    data = futu_sql.select(
        sql="""
        select * from test_table_1
        where arrival > '2020-01-01'
        """,
    )
    futu_sql.create_table(
        sql="""
        CREATE TABLE `test_table_4` (
          `orderdate` date DEFAULT NULL,
          `arrival` date DEFAULT NULL,
          `date_diff` int(11) DEFAULT NULL,
          `ordernum` int(11) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='test data 2'
        """
    )
    # futu_sql.insert(
    #     tbl_name='test_table_4',
    #     df=data.iloc[:5333, :],
    #     col_list=['orderdate', 'arrival', 'date_diff',
    #               # 'ordernum'
    #               ]
    # )
    # futu_sql.update(
    #     tbl_name='test_table_4',
    #     df=data.iloc[[4], :],
    #     col_list=[
    #         'orderdate',
    #         # 'arrival',
    #         # 'date_diff',
    #         'ordernum',
    #     ],
    #     conditions="""where arrival >= '2020-02-01'"""
    # )
    #
    #
    #
