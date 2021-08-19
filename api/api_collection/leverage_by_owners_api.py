from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import pandas as pd
import psycopg2
import json

class Api(BaseApi):
    """
    这个 api 的访问路径为：/test/hello
    无需定义路由，文件路径即 api 路径。
    """

    def run(self, input) -> ApiOutput:
        """
        这里的 input 为 api 入参，类型为 dict。
        """


        def from_sql_to_df (sql_x):
            cur.execute(sql_x)
            x = cur.fetchall()
            x_df = pd.DataFrame(list(x))
            return x_df


        # connect psql
        DB_NAME = "cexplorer"
        DB_HOST = "/var/run/postgresql"
        DB_USER = "siri"
        DB_PASS = "PasswordYouWant"
        DB_PORT =  "5432"

        conn = psycopg2.connect(
                                    dbname   =   DB_NAME,
                                    host     =   DB_HOST,
                                    user     =   DB_USER,
                                    password =   DB_PASS,
                                    port = DB_PORT
                        )
        # define cursor
        cur = conn.cursor()
            # print cursor information
            # print(conn.get_dsn_parameters(),"\n")



        #  各个epoch里pool_onwer 的信息
        sql_pool_owner_addr = 'select epoch_no, pool_hash_id, addr_id from v_pool_owners_by_epoch;'
        pool_owner_addr = from_sql_to_df(sql_pool_owner_addr)
        pool_owner_addr.columns = [ "epoch_no", "pool_hash_id",  "addr_id"]

            # 各个epoch里的有效池的hash_id 和pledge
        sqp_valid_pools_each_epoch = "select pool_hash_id, epoch_no, active_pledge from V_POOL_HISTORY_BY_EPOCH;"
        pool_pledge = from_sql_to_df(sqp_valid_pools_each_epoch)
        pool_pledge.columns = ["pool_hash_id", "epoch_no", "pledge_amount"]
        pool_pledge["pledge_amount"] = pool_pledge["pledge_amount"].fillna(0)


        # 找到0 pledge的
        # pool_pledge_is_0 = pool_pledge[pool_pledge["pledge_amount"] == 0]

        # 计算每一个owner 的pledge总数
        # Onwer 和 pledge 对应起来: owner, epoch_no, amount of pledge
        owner_pledge = pd.merge(pool_owner_addr,pool_pledge, on = ["pool_hash_id","epoch_no"], how = "inner")
        owner_pledge = owner_pledge.loc[:,["addr_id","epoch_no","pledge_amount"]]
        owner_pledge = owner_pledge.groupby(by=["addr_id","epoch_no"])["pledge_amount"].sum()
        owner_pledge = owner_pledge.to_frame()
        owner_pledge = owner_pledge.reset_index()
        owner_pledge.columns = ["addr_id","epoch_no","pledge_amount"]


        # 计算每一个owner 的delegation 总数
        # 所有delegation的历史
        sql_delegation_history = "select stake_address.id as addr_id, epoch_stake.epoch_no, epoch_stake.amount from stake_address inner join epoch_stake on stake_address.id = epoch_stake.addr_id;"
        delegation_history = from_sql_to_df(sql_delegation_history)
        delegation_history.columns = [ "addr_id","epoch_no", "delegation_amount"]

        # 每一个owner的delegation
        pool_owner_delegation = pd.merge(pool_owner_addr,delegation_history, on = ["addr_id","epoch_no"], how="inner")    

        # 对大部分没有delegation历史的owner的delegation_amount填0
        pool_owner_delegation["delegation_amount"] = pool_owner_delegation["delegation_amount"].fillna(0)

        # 计算每一个owner 的delegation的总数
        pool_owner_delegation = pool_owner_delegation.loc[:,["addr_id","epoch_no","delegation_amount"]]
        pool_owner_delegation = pool_owner_delegation.groupby(by=["addr_id","epoch_no"])["delegation_amount"].sum()
        pool_owner_delegation = pool_owner_delegation.to_frame()
        pool_owner_delegation = pool_owner_delegation.reset_index()
        pool_owner_delegation.columns = ["addr_id","epoch_no","delegation_amount"]


        # 计算每一个owner 管理的pool 的amount 总数
        # pool / amount / epoch
        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]
        pool_pledge_amount = pd.merge(pool_owner_addr,stake_distribution, on =["pool_hash_id","epoch_no"], how = "inner")
        pool_pledge_amount = pool_pledge_amount.loc[:,["pool_hash_id","epoch_no","pool_amount"]]
        pool_pledge_amount = pool_pledge_amount.groupby(by=["pool_hash_id","epoch_no"])["pool_amount"].sum()
        pool_pledge_amount = pool_pledge_amount.to_frame()
        pool_pledge_amount = pool_pledge_amount.reset_index()
        pool_pledge_amount.columns = ["pool_hash_id","epoch_no","pool_amount"]

        # owner     epoch       amount
        owner_amount = pd.merge(pool_pledge_amount, pool_owner_addr, on = ["epoch_no",'pool_hash_id'], how ="left" )
        owner_amount = owner_amount.loc[:,["epoch_no","addr_id","pool_amount"]]
        owner_amount = owner_amount.groupby(by=["epoch_no","addr_id"])["pool_amount"].sum()
        owner_amount = owner_amount.to_frame()
        owner_amount = owner_amount.reset_index()
        owner_amount.columns = ["epoch_no","addr_id","pool_amount"]


        # owner     epoch   pledge  delegation  amount
        leverage = pd.merge(owner_pledge,pool_owner_delegation, on=["epoch_no","addr_id"], how = "inner")
        leverage = pd.merge(leverage, owner_amount, on = ['epoch_no','addr_id'], how = "inner")

        # 转换数据类型
        leverage["pledge_amount"] = leverage["pledge_amount"].astype('float64')
        leverage["pool_amount"] = leverage["pool_amount"].astype('float64')
        leverage["delegation_amount"] = leverage["delegation_amount"].astype('float64')
        leverage.insert(leverage.shape[1], 'leverage', 0)

        epoch_list = leverage['epoch_no'].drop_duplicates().values.tolist()  

        kong_df = pd.DataFrame(columns=["pool_hash_id","epoch_no","pledge_amount","pool_amount","delegation_amount","leverage"])

        for selected_epoch in epoch_list:
            selected_df = leverage[leverage["epoch_no"] ==selected_epoch ]
            selected_df["leverage"] =  selected_df.apply(lambda y:  (
                y['pool_amount']-y['delegation_amount'])//y['pledge_amount']  
                    if  ( 
                        y['pledge_amount'] != 0 ) else "No Pledge" ,  axis=1)
            df = pd.concat([kong_df,selected_df])
        result = df.to_json(orient="records")
        parsed = json.loads(result)


        cur.close()
        conn.close()


        return ApiOutput.success(parsed)





