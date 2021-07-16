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
        cur = conn.cursor()
        cur.execute("SELECT version();") 

    
        def from_sql_to_df (sql_x):
            cur.execute(sql_x)
            x = cur.fetchall()
            x_df = pd.DataFrame(list(x))
            return x_df

    # current valid pools:
        sql_current_valid_pools = """
            select  hash_id, pledge, active_epoch_no,  registered_tx_id , reward_addr
            from pool_update where 
                registered_tx_id in 
                (select 
                    max(registered_tx_id) from pool_update group by hash_id) 
                    and not exists ( 
                        select * from pool_retire where pool_retire.hash_id = pool_update.hash_id and pool_retire.retiring_epoch <= (select max (epoch_no) from block));
            """
        current_valid_pools = from_sql_to_df(sql_current_valid_pools)
        current_valid_pools.columns =  [ "pool_hash_id", "pledge", "active_epoch", "registered_tx_id", "reward_addr"]    
        current_valid_pools.to_csv("/home/siri/Desktop/current_valid_pools.csv")

        current_valid_pools_short = current_valid_pools.loc[:, [ "pool_hash_id", "registered_tx_id"]]
        # print("Current valid pools are: ", "\n", current_valid_pools.head() , "\n")


    #  各个epoch里pool_onwer 的信息
        sql_pool_owner_addr = 'select epoch_no, pool_hash_id, addr_id from v_pool_owners_by_epoch;'
        pool_owner_addr = from_sql_to_df(sql_pool_owner_addr)
        pool_owner_addr.columns = [ "epoch_no", "pool_hash_id",  "addr_id"]
        # print("The list of pool owners:", "\n", pool_owner_addr.head(), "\n")

    # 各个epoch里的有效池的hash_id 和pledge
        sqp_valid_pools_each_epoch = "select pool_hash_id, epoch_no, active_pledge from V_POOL_HISTORY_BY_EPOCH;"
        pool_pledge = from_sql_to_df(sqp_valid_pools_each_epoch)
        pool_pledge.columns = ["pool_hash_id", "epoch_no", "pledge_amount"]


    # 各个epoch里有效池的stake amount
    # 有一些pool 里有pledge记录但是没有amount{}
        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]
        # print("stake_distribution", "\n", stake_distribution.head())
    # 所有delegation的历史，不考虑过期的情况｛point｝
    # # delegation 是有利息的，同一笔delegation会逐渐增加，操
        sql_delegation_history = "select stake_address.id as addr_id, epoch_stake.epoch_no, epoch_stake.amount from stake_address inner join epoch_stake on stake_address.id = epoch_stake.addr_id;"
        delegation_history = from_sql_to_df(sql_stake_distribution)
        delegation_history.columns = [ "addr_id","epoch_no", "delegation_amount"]
        # print("delegation_history", "\n", delegation_history.head())

    # pool_owner 的delegation amount历史
        pool_owner_delegation = pd.merge(pool_owner_addr,delegation_history, on = ["addr_id","epoch_no"], how="left")
        # print("pool_owner_delegation", '\n',pool_owner_delegation.head(20))

        # 选择出有用的数据
        pool_delegation =  pool_owner_delegation.loc[:,["epoch_no", "pool_hash_id","delegation_amount"]] 

        # 对每一个pool 对应的delegation求和
        pool_delegation = pool_owner_delegation.groupby(by = ["epoch_no", "pool_hash_id"], sort = False)["delegation_amount"].sum()
        # print("pool_delegation", "\n", pool_delegation.head())
        
        
        pool_pledge_amount = pd.merge(pool_pledge,stake_distribution, on =["pool_hash_id","epoch_no"], how = "inner")
        pool_pledge_amount_delegation = pd.merge(pool_pledge_amount,pool_delegation, on=["pool_hash_id","epoch_no"], how = "left")

        # 对于没有pledge数据的pledge填0 处理，后面要找出这些是0的池子，看一下它们是什么情况
        pool_pledge_amount_delegation["pledge_amount"] = pool_pledge_amount_delegation["pledge_amount"].fillna(0)
        pool_pledge_is_0 = pool_pledge_amount_delegation[pool_pledge_amount_delegation["pledge_amount"] == 0]
        # 对大部分没有delegation历史的owner的delegation_amount填0
        pool_pledge_amount_delegation["delegation_amount"] = pool_pledge_amount_delegation["delegation_amount"].fillna(0)
        # pool_pledge_is_0.to_csv("/home/siri/Desktop/pool_pledge_is_0.csv")
        # pool_pledge_amount_delegation.to_csv("/home/siri/Desktop/pool_pledge_amount_delegation.csv")

        # 转换数据类型
        pool_pledge_amount_delegation["pledge_amount"] = pool_pledge_amount_delegation["pledge_amount"].astype('float64')
        pool_pledge_amount_delegation["pool_amount"] = pool_pledge_amount_delegation["pool_amount"].astype('float64')
        pool_pledge_amount_delegation["delegation_amount"] = pool_pledge_amount_delegation["delegation_amount"].astype('float64')
        pool_pledge_amount_delegation.insert(pool_pledge_amount_delegation.shape[1], 'leverage', 0)

        epoch_list = pool_pledge_amount_delegation['epoch_no'].drop_duplicates().values.tolist()  

        kong_df= pd.DataFrame(columns=["pool_hash_id","epoch_no","pledge_amount","pool_amount","delegation_amount","pledge_ratio"])

    # Pledge ratio by epoch  by pools
        for selected_epoch in epoch_list:
            selected_df = pool_pledge_amount_delegation[pool_pledge_amount_delegation["epoch_no"] ==selected_epoch ]
            selected_df["pledge_ratio"] =  selected_df.apply(lambda y:  y['pledge_amount']-y['pool_amount'] , axis=1)
            kong_df = kong_df.append(selected_df,  ignore_index=True)
            # here is the pledge ratio of all pools by epoch

        result = kong_df.to_json(orient="records")
        parsed = json.loads(result)

        cur.close()
        conn.close()

        # 访问数据库的操作封装起来，一处定义，多处使用
        # db.query_as_pd("SELECT * from table")
        # db.query("SELECT * from table")

        return ApiOutput.success(parsed)
