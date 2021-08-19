from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import pandas as pd
import psycopg2
import json
# 设置任何人都可以访问


class Api(BaseApi):
    """
    这个 api 的访问路径为：/test/hello
    无需定义路由，文件路径即 api 路径。
    """
    
    def run(self, input) -> ApiOutput:
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

        # 各个epoch里的有效池的hash_id 和pledge
        sqp_valid_pools_each_epoch = "select pool_hash_id, epoch_no, active_pledge from V_POOL_HISTORY_BY_EPOCH;"
        pool_pledge = from_sql_to_df(sqp_valid_pools_each_epoch)
        pool_pledge.columns = ["pool_hash_id", "epoch_no", "pledge_amount"]


        # 各个epoch里有效池的stake amount
        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]


        pledge_amount = pd.merge(pool_pledge,stake_distribution, on = ["epoch_no","pool_hash_id"] )
        pledge_amount['pledge_amount'] = pledge_amount['pledge_amount'].astype(float)
        pledge_amount['pool_amount']= pledge_amount['pool_amount'].astype(float)


        pledge_amount.insert(pledge_amount.shape[1], 'pledge_rate', 0)

        pledge_amount = pledge_amount[pledge_amount['pool_amount']>0]

        pledge_amount["pledge_rate"] =  pledge_amount.apply(lambda y : y['pledge_amount']/y['pool_amount'] , axis=1)
        result = pledge_amount.to_json(orient="records")
        parsed = json.loads(result)

        cur.close()
        conn.close()



        return ApiOutput.success(parsed)
