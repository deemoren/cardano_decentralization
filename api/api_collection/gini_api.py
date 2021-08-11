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
    def run(self,input) -> ApiOutput:
        DB_NAME = "cexplorer"
        DB_HOST = "/var/run/postgresql"
        DB_USER = "siri"
        DB_PASS = "PasswordYouWant"
        DB_PORT =  "5432"
        """
        这里的 input 为 api 入参，类型为 dict。
        """
        
        conn = psycopg2.connect(
                                    dbname   =   DB_NAME,
                                    host     =   DB_HOST,
                                    user     =   DB_USER,
                                    password =   DB_PASS,
                                    port = DB_PORT
                        )
        # define cursor
        cur = conn.cursor()

        def from_sql_to_df (sql_x):
            cur.execute(sql_x)
            x = cur.fetchall()
            x_df = pd.DataFrame(list(x))
            return x_df
        def get_gini(block_pool, selected_epoch):
            selected_block_pool = block_pool.loc[block_pool["epoch_no"] == selected_epoch]
            selected_block_pool = selected_block_pool.loc[:,["pool_hash_id","block_amount"]]

            # 唯一pool_hash_id 的数量
            A = selected_block_pool.shape[0]
            sigam_block = selected_block_pool["block_amount"].sum()

            deviation_NB = 0
            for i  in range(A-1):
                for j in range(A-1):
                    NB_A_i = selected_block_pool.iloc[i,1]
                    NB_A_j = selected_block_pool.iloc[j,1]
                    deviation_NB = abs(NB_A_i - NB_A_j) + deviation_NB
            gini_coefficient = deviation_NB / (sigam_block*2*A)
            return selected_epoch, gini_coefficient

        #   Metric5 : Gini Coefficient
        # Get the block number of blocks created in an epoch by all pools
        sql_block_pool = """
        select block.epoch_no, pool_hash.id, count (*) as block_count from block 
        inner join slot_leader on block.slot_leader_id = slot_leader.id
        inner join pool_hash on slot_leader.pool_hash_id = pool_hash.id
        group by block.epoch_no, pool_hash.id ;
        """
        block_pool = from_sql_to_df(sql_block_pool)
        block_pool.columns = [ "epoch_no", "pool_hash_id", "block_amount"]

        # get valid epoch_no
        epoch_list = block_pool['epoch_no'].drop_duplicates().values.tolist()
    # calculate the gini_coefficient of all valid epoch
        gini_list = []
        for selected_epoch in epoch_list:
            temp_tuple = get_gini(block_pool, selected_epoch)
            gini_list.append(temp_tuple)
        gini_df = pd.DataFrame(gini_list)
        gini_df.columns = ["epoch_no", "Gini_Coefficient"]
        result = gini_df.to_json(orient="records")
        parsed = json.loads(result)

        cur.close()
        conn.close()
        # 访问数据库的操作封装起来，一处定义，多处使用
        # db.query_as_pd("SELECT * from table")
        # db.query("SELECT * from table")

        return ApiOutput.success(parsed)
