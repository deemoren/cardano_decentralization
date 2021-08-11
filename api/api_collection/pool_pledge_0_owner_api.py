from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import psycopg2
import json
import pandas as pd

class Api(BaseApi):

    def run(self, input) -> ApiOutput:

        DB_NAME = "cexplorer"
        DB_HOST = "/var/run/postgresql"
        DB_USER = "siri"
        DB_PASS = "PasswordYouWant"
        DB_PORT =  "5432"

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

    #  pool owners of current valid pools ,and of pools on selected epoch
        sql_pool_owner_addr = """
        select epoch_no, pool_hash_id, addr_id from v_pool_owners_by_epoch;
        """
        pool_owner_addr = from_sql_to_df(sql_pool_owner_addr)
        pool_owner_addr.columns = [ "epoch_no", "pool_hash_id",  "owner_address_id"]

        # Get the block number of blocks created in an epoch by all pools

        sql_block_pool = """
        select block.epoch_no, pool_hash.id, count (*) as block_count from block 
        inner join slot_leader on block.slot_leader_id = slot_leader.id
        inner join pool_hash on slot_leader.pool_hash_id = pool_hash.id
        group by block.epoch_no, pool_hash.id ;
        """
        block_pool = from_sql_to_df(sql_block_pool)
        block_pool.columns = [ "epoch_no", "pool_hash_id", "block_amount"]

        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]

        # get valid epoch_no
        epoch_list = block_pool['epoch_no'].drop_duplicates().values.tolist()


    # get the pool_amount of pools by epoch
        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]


        sqp_valid_pools_each_epoch = "select pool_hash_id, epoch_no, active_pledge from V_POOL_HISTORY_BY_EPOCH;"
        pool_pledge = from_sql_to_df(sqp_valid_pools_each_epoch)
        pool_pledge.columns = ["pool_hash_id", "epoch_no", "pledge"]

        pool_pledge_0 = pool_pledge[pool_pledge["pledge"] == 0]
        pool_pledge_0_owner = pool_pledge_0.merge(pool_owner_addr, on =["epoch_no","pool_hash_id"], how = "left")
        pool_pledge_0_owner = pool_pledge_0_owner.merge(block_pool,  on =["epoch_no","pool_hash_id"], how = "left")
        pool_pledge_0_owner = pool_pledge_0_owner.merge(stake_distribution, on =["epoch_no","pool_hash_id"], how = "left")

        # plus pool.view
        sql_view = 'select id,view from pool_hash;'
        view = from_sql_to_df(sql_view)
        view.columns = ["pool_hash_id","view"]
        pool_pledge_0_owner = pool_pledge_0_owner.merge(view, on = ["pool_hash_id"], how = "left")

        result = pool_pledge_0_owner.to_json(orient="records")
        parsed = json.loads(result)

        # dis conn
        cur.close()
        conn.close()


        return ApiOutput.success(parsed)
