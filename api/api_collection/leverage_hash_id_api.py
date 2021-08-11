from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import pandas as pd
import psycopg2
import json



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

        def from_sql_to_df (sql_x):
                cur.execute(sql_x)
                x = cur.fetchall()
                x_df = pd.DataFrame(list(x))
                return x_df



        sqp_valid_pools_each_epoch = "select pool_hash_id, epoch_no, active_pledge from V_POOL_HISTORY_BY_EPOCH;"
        pool_pledge = from_sql_to_df(sqp_valid_pools_each_epoch)
        pool_pledge.columns = ["pool_hash_id", "epoch_no", "pledge_amount"]
        

        sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
        stake_distribution = from_sql_to_df(sql_stake_distribution)
        stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]
        
        # amount + pledge: pool_hash_id, epoch_no, pool_amount, pledge_amount
        pool_pledge_amount = pd.merge(pool_pledge,stake_distribution, on = ['pool_hash_id','epoch_no'], how='left')



        sql_pool_owner_addr = 'select epoch_no, pool_hash_id, addr_id from v_pool_owners_by_epoch;'
        pool_owner_addr = from_sql_to_df(sql_pool_owner_addr)
        pool_owner_addr.columns = [ "epoch_no", "pool_hash_id",  "addr_id"]


        # pool_hash_id, epoch_no, pool_amount, pledge_amount, addr_id
        pledge_amount_owner = pd.merge(pool_pledge_amount,pool_owner_addr, on = ['pool_hash_id','epoch_no'], how='left')


        # delegation history of  everyone
        sql_delegation_history = "select stake_address.id as addr_id, epoch_stake.epoch_no, epoch_stake.amount from stake_address inner join epoch_stake on stake_address.id = epoch_stake.addr_id;"
        delegation_history = from_sql_to_df(sql_delegation_history)

        # delegation history of pool owners
        delegation_history.columns = [ "addr_id","epoch_no", "delegation_amount"]
        pool_owner_delegation = pd.merge(pool_owner_addr,delegation_history, on = ["addr_id","epoch_no"], how="left")

        # fillna 0 for NAN delegation_amount
        pool_owner_delegation['delegation_amount'] = pool_owner_delegation["delegation_amount"].fillna(0)
        owner_delegation_byepoch = pool_owner_delegation.groupby(by=["epoch_no", "pool_hash_id",'addr_id'], sort=False)['delegation_amount'].sum()
        owner_delegation_byepoch.to_frame()
        owner_delegation_byepoch.reset_index()


        # add delegation amount of each owner to .df
        leverage = pd.merge(pledge_amount_owner,owner_delegation_byepoch, on=['pool_hash_id','epoch_no','addr_id'], how='outer')
        leverage = leverage[leverage['epoch_no'] > 210]
        leverage = leverage.drop_duplicates()




        leverage["pledge_amount"] = leverage["pledge_amount"].fillna(0)
        pool_pledge_is_0 = leverage[leverage["pledge_amount"] == 0]


        leverage_delegation = leverage.loc[:,['pool_hash_id', 'epoch_no', 'delegation_amount']]
        leverage_delegation = leverage_delegation.groupby(by = ['pool_hash_id', 'epoch_no'])['delegation_amount'].sum()
        leverage_delegation .to_frame()
        leverage_delegation.reset_index()


        leverage_count = pd.merge(pool_pledge_amount, leverage_delegation, on=['epoch_no','pool_hash_id'])



        leverage_count["pledge_amount"] = leverage_count["pledge_amount"].astype('float')
        leverage_count["pool_amount"] = leverage_count["pool_amount"].astype('float')
        leverage_count["delegation_amount"] = leverage_count["delegation_amount"].astype('float')
        leverage_count.insert(leverage_count.shape[1], 'leverage', 0)
        leverage_count["leverage"] =   leverage_count.apply(lambda y: (
            y['pool_amount']-y['delegation_amount'])//y['pledge_amount'] 
                if 
                ( y['pledge_amount'] != 0 ) 
                    else "No Pledge" , 
                    axis = 1)

        result = leverage_count.to_json(orient="records")
        parsed = json.loads(result)
        cur.close()
        conn.close()

        return ApiOutput.success(parsed)
