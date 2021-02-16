from futu import *
from mysql_conn_test import *
import datetime
import time
import random
import numpy as np
import math


class StockGetter:

    MARKET_CONST_DICT = {
        'None': Market.HK,
        "HK": Market.HK,
        "US": Market.US,
        "SH": Market.SH,
        "SZ": Market.SZ,
        "HK_FUTURE": Market.HK_FUTURE
    }
    MARKET_TBL_DICT = {
        'None': 'dimnone_stock_info',
        "HK": 'dim_hk_stock_info',
        "US": 'dim_us_stock_info',
        "SH": 'dim_sh_stock_info',
        "SZ": 'dim_sz_stock_info',
        "HK_FUTURE": 'dim_hkfuture_stock_info'
    }

    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 11112
        self.__have_data = 0
        self.__data = None


class StockInfoGetter(StockGetter):

    def __init__(
            self,
            market,
    ):
        super(StockInfoGetter, self).__init__()
        self.market_cls = StockGetter.MARKET_CONST_DICT[market]
        self.tbl_name = StockGetter.MARKET_TBL_DICT[market]

    def get_stock_info_data(self):
        quote_ctx = OpenQuoteContext(host=self.host, port=self.port)  # 创建行情对象

        for retry_i in range(11):
            ret, data = quote_ctx.get_stock_basicinfo(self.market_cls, SecurityType.STOCK)
            if ret == RET_OK:
                quote_ctx.close()
                data['create_date'] = datetime.date.today()
                data['last_change_datetime'] = datetime.datetime.now()
                data.replace({'': None, 'N/A': None, np.nan: None}, inplace=True)
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

    def get_data(self):
        return self.__data

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


class StockSnapshotGetter(StockGetter):

    BATCH_SIZE = 20

    def __init__(self):
        super(StockSnapshotGetter, self).__init__()

    def get_stock_snapshot(
            self,
            stock_list: list,
    ):
        ss_data = pd.DataFrame()
        quote_ctx = OpenQuoteContext(host=self.host, port=self.port)
        for retry_i in range(11):
            ret, ss_data = quote_ctx.get_market_snapshot(stock_list)
            if ret == RET_OK:
                quote_ctx.close()
                ss_data['snapshot_time'] = datetime.datetime.now()
                ss_data.replace({'': None, 'N/A': None, np.nan: None}, inplace=True)
                break
            else:
                quote_ctx.close()
                if retry_i < 10:
                    print('Data not fetched. Retry for the ' + str(retry_i + 1) + 'st time...')
                    time.sleep(0.501 + random.random() * 0.01)
                else:
                    print('Failed to get data!')
        return ss_data

    def get_all_market_snapshot(self, market, src='api'):

        if src == 'db':
            futu_sql = MySQLTool()
            futu_sql.ali_connect()
            all_stock_code = futu_sql.select(
                sql="""
                        select distinct code from %s
                        where delisting = 0
                        """ % StockGetter.MARKET_TBL_DICT[market],
            ).squeeze().tolist()
        else:
            info_getter = StockInfoGetter(market=market)
            info_getter.get_stock_info_data()
            all_stock_code = info_getter.get_data().code.tolist()

        stock_num = all_stock_code.__len__()
        batch_num = math.ceil(stock_num / StockSnapshotGetter.BATCH_SIZE)
        data_df = pd.DataFrame()
        for b_i in range(batch_num):
            time.sleep(0.501 + random.random() * 0.01)
            print('batch: ' + str(b_i))
            new_ss = self.get_stock_snapshot(
                stock_list=all_stock_code[
                    b_i * StockSnapshotGetter.BATCH_SIZE: (b_i + 1) * StockSnapshotGetter.BATCH_SIZE
                ]
            )
            if new_ss.shape[0] == 0:
                raise ValueError
            else:
                data_df = pd.concat((data_df, new_ss), axis=0)

        return data_df


if __name__ == '__main__':

    tst = StockSnapshotGetter()
    df = tst.get_all_market_snapshot(market='SZ', src='futu')



    # tst = StockInfoGetter(market='SZ')
    # tst.get_stock_info_data()
    # tst.load_into_mysql()

    # futu_sql = MySQLTool()
    # futu_sql.ali_connect()
    # hk_stock_list = futu_sql.select(
    #     sql="""
    #         select distinct code from dim_hk_stock_info
    #         where delisting = 0
    #         """,
    # )
    #
    # quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11112)
    #
    # ret, data = quote_ctx.get_market_snapshot(hk_stock_list.squeeze().tolist()[:20])
    # if ret == RET_OK:
    #     pass
    # else:
    #     print('error:', data)
    # quote_ctx.close()  # 结束后记得关闭当条连接，防止连接条数用尽




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


