from futu import *
from mysql_conn_test import *
import datetime
import time
import random


class StockInfoGetter:

    market_const_dict = {
        'None': Market.HK,
        "HK": Market.HK,
        "US": Market.US,
        "SH": Market.SH,
        "SZ": Market.SZ,
        "HK_FUTURE": Market.HK_FUTURE
    }
    market_tbl_dict = {
        'None': 'dimnone_stock_info',
        "HK": 'dim_hk_stock_info',
        "US": 'dim_us_stock_info',
        "SH": 'dim_sh_stock_info',
        "SZ": 'dim_sz_stock_info',
        "HK_FUTURE": 'dim_hkfuture_stock_info'
    }

    def __init__(
            self,
            market,
    ):
        self.market_cls = StockInfoGetter.market_const_dict[market]
        self.host = '127.0.0.1'
        self.port = 11112
        self.__have_data = 0
        self.__data = None
        self.tbl_name = StockInfoGetter.market_tbl_dict[market]

    def get_stock_info_data(self):
        quote_ctx = OpenQuoteContext(host=self.host, port=self.port)  # 创建行情对象

        for retry_i in range(11):
            ret, data = quote_ctx.get_stock_basicinfo(self.market_cls, SecurityType.STOCK)
            if ret == RET_OK:
                quote_ctx.close()
                data['create_date'] = datetime.date.today()
                data['last_change_datetime'] = datetime.datetime.now()
                data.replace({'': None, 'N/A': None}, inplace=True)
                self.__have_data = 1
                self.__data = data
                break
            else:
                quote_ctx.close()
                if retry_i < 10:
                    print('Data not fetched. Retry for the ' + str(retry_i + 1) + 'st time...')
                    time.sleep(1 + random.random() * 2)
                else:
                    print('Failed to get data!')
                return None

    def load_into_mysql(self):
        if self.__have_data == 0:
            print('No data to load!')
        else:
            futu_sql = MySQLTool()
            futu_sql.ali_connect()
            futu_sql.insert(
                tbl_name=self.tbl_name,
                df=self.__data,
            )


if __name__ == '__main__':

    tst = StockInfoGetter(market='SH')
    tst.get_stock_info_data()
    tst.load_into_mysql()




    # quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11112)  # 创建行情对象
    # ret, data = quote_ctx.get_stock_basicinfo(Market.HK, SecurityType.STOCK)
    # quote_ctx.close()  # 关闭对象，防止连接条数用尽
    # data['create_date'] = datetime.date.today()
    # data['last_change_datetime'] = datetime.datetime.now()
    # data.replace({'': None, 'N/A': None}, inplace=True)

    # futu_sql = MySQLTool()
    # futu_sql.ali_connect()
    # futu_sql.insert(
    #     tbl_name='dim_hk_stock_info',
    #     df=data,
    # )

    # quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11112)  # 创建行情对象
    # ret, data_2 = quote_ctx.get_market_snapshot(data.code[:10, ].tolist())
    # if ret == RET_OK:
    #     print(data)
    #     print(data['code'][0])  # 取第一条的股票代码
    #     print(data['code'].values.tolist())  # 转为list
    # else:
    #     print('error:', data)
    # quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽


