from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import pandas as pd
import psycopg2
import math
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
        # 访问数据库的操作封装起来，一处定义，多处使用
        # db.query_as_pd("SELECT * from table")
        # db.query("SELECT * from table")
    #try conn
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
                
        def shannon_entropy(block_pool, selected_epoch):
            selected_block_pool = block_pool.loc[block_pool["epoch_number"] == selected_epoch]
            selected_block_pool = selected_block_pool.loc[:,["pool_hash_id","block_amount"]]
            selected_block_pool = selected_block_pool.reindex(selected_block_pool['block_amount'].sort_values(ascending=False).index)
            # How many pool_hash_id in total
            N = selected_block_pool.shape[0]
            sigam_block = selected_block_pool["block_amount"].sum()
            Shanno_Entropy = 0
            sigma_p_i = 0
            KL = 0
            for i  in range(N-1):
                NB_A_i = selected_block_pool.iloc[i,1]
                p_i =    NB_A_i / sigam_block
                sigma_p_i = p_i + sigma_p_i
                KL_d = p_i * math.log( p_i  ,2) - p_i* math.log((1/N),2  )
                KL = KL + KL_d
                if sigma_p_i <= (0.5000):
                    ranking = i
                Shanno_Entropy = Shanno_Entropy - ( p_i * ( math.log(p_i, 2)) )
                range_max = math.log(N,2)
            return selected_epoch, Shanno_Entropy, range_max, ranking, N, KL

        sql_block_pool = """
        select block.epoch_no, pool_hash.id, count (*) as block_count from block 
        inner join slot_leader on block.slot_leader_id = slot_leader.id
        inner join pool_hash on slot_leader.pool_hash_id = pool_hash.id
        group by block.epoch_no, pool_hash.id ;
        """
        block_pool = from_sql_to_df(sql_block_pool)
        block_pool.columns = [ "epoch_number", "pool_hash_id", "block_amount"]
        # get valid epoch_no
        epoch_list = block_pool['epoch_number'].drop_duplicates().values.tolist()

    # Calculate Shannon Entropy
        shannon_list= []
        for selected_epoch in epoch_list:
            shannon_tuple = shannon_entropy(block_pool, selected_epoch)
            shannon_list.append(shannon_tuple)
        shannon_df = pd.DataFrame(shannon_list)
        shannon_df.columns = ["epoch_no", "Shanno_Entropy", "max_value_of_SE","51_attack_colluders","pool_numbers","KL"]
        result = shannon_df.to_json(orient="records")
        parsed = json.loads(result)
        cur.close()
        conn.close()

        return ApiOutput.success(parsed)


