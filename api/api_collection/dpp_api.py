from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import pandas as pd
import psycopg2
import json
import math



class Api(BaseApi):


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
        # define cursor
        cur = conn.cursor()
        cur.execute("SELECT version();") 
        record=cur.fetchone()

        def from_sql_to_df (sql_x):
            cur.execute(sql_x)
            x = cur.fetchall()
            x_df = pd.DataFrame(list(x))
            return x_df
        def count_delegator(delegator_pool, selected_epoch):
            selected_delegator_pool = delegator_pool[delegator_pool["epoch_no"] == selected_epoch]
            selected_delegator_pool = selected_delegator_pool.loc[:,["addr_id","pool_hash_id"]]
            selected_delegator_pool = selected_delegator_pool.drop_duplicates()
            temp = selected_delegator_pool.groupby(by = "pool_hash_id")["addr_id"].count()
            x = temp.sum()
            y = temp.shape[0]
            z = x/y
            return selected_epoch,  x,  y,  z
    


        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]
   

        sql_delegation_history = "select stake_address.id as addr_id, epoch_stake.epoch_no, epoch_stake.amount from stake_address inner join epoch_stake on stake_address.id = epoch_stake.addr_id;"
        delegation_history = from_sql_to_df(sql_stake_distribution)
        delegation_history.columns = [ "addr_id","epoch_no", "delegation_amount"]


    
    # delegator info
        sql_delegator_pool = "select stake_address.id, delegation.active_epoch_no, pool_hash.id pool_hash_id from delegation inner join stake_address on delegation.addr_id = stake_address.id inner join pool_hash on delegation.pool_hash_id = pool_hash.id;"
        delegator_pool = from_sql_to_df(sql_delegator_pool)
        delegator_pool.columns = ["addr_id","epoch_no", "pool_hash_id"]

        epoch_list = delegator_pool['epoch_no'].drop_duplicates().values.tolist()  

    
        delegators_per_pool = []
        for selected_epoch in epoch_list:
            temp_tuple = count_delegator(delegator_pool, selected_epoch)
            delegators_per_pool.append(temp_tuple)

    # Pledge ratio by epoch  by pools

        delegators_per_pool = pd.DataFrame(delegators_per_pool)
        delegators_per_pool.columns = ["eopch_no","delegators_numbers","pool_bumbers", "delegator_per_pool"]
        result = delegators_per_pool.to_json(orient="records")
        parsed = json.loads(result)
        cur.close()
        conn.close()

        return ApiOutput.success(parsed)
