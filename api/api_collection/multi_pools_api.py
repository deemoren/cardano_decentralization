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
        # define cursor
        cur = conn.cursor()
        cur.execute("SELECT version();") 
        
        def from_sql_to_df (sql_x):
            cur.execute(sql_x)
            x = cur.fetchall()
            x_df = pd.DataFrame(list(x))
            return x_df


    #  pool owners of current valid pools ,and of pools on selected epoch
        sql_pool_owner_addr = """
        select epoch_no, pool_hash_id, addr_id from v_pool_owners_by_epoch;
        """
        pool_owner_addr = from_sql_to_df(sql_pool_owner_addr)
        pool_owner_addr.columns = [ "epoch_no", "pool_hash_id",  "owner_address_id"]
        # print("The list of pool owners:", "\n", pool_owner_addr.head(), "\n")

        sqp_adr = "select id, view from stake_address;"
        adr_id = from_sql_to_df(sqp_adr)
        adr_id.columns = ["addr_id", "stake_address"]
        pool_owner_addr  =  pd.merge(pool_owner_addr, adr_id, left_on = "owner_address_id",right_on="addr_id", how = "left")

        owner_multiple_pools = pool_owner_addr.groupby(by = ["epoch_no","owner_address_id","stake_address"])["pool_hash_id"].count()
        result = owner_multiple_pools.to_json(orient="index")
        parsed = json.loads(result)

        cur.close()
        conn.close()


        # 访问数据库的操作封装起来，一处定义，多处使用
        # db.query_as_pd("SELECT * from table")
        # db.query("SELECT * from table")

        return ApiOutput.success(parsed)

