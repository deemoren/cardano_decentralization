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
        cur = conn.cursor()
        cur.execute("SELECT version();") 
    
        def from_sql_to_df (sql_x):
            cur.execute(sql_x)
            x = cur.fetchall()
            x_df = pd.DataFrame(list(x))
            return x_df
  
    
        sql_pledge_epoch = "select epoch_no, sum(active_pledge) from v_pool_history_by_epoch where epoch_no>210 and pool_hash_id <> 5806 and active_pledge>0 group by epoch_no;"
        pledge_epoch = from_sql_to_df(sql_pledge_epoch)
        pledge_epoch.columns = ["epoch_no","pledge"]
        sql_amount = 'select epoch_no,sum(amount) from epoch_stake where pool_id <> 5806 group by epoch_no order by sum desc;'
        amount = from_sql_to_df(sql_amount)
        amount.columns = ["epoch_no","amount"]
        pledge_rate = pd.merge(pledge_epoch,amount,on=["epoch_no"], how = "inner")
        pledge_rate["pledge"] =     pledge_rate["pledge"].astype('float64' )
        pledge_rate["amount"] =     pledge_rate["amount"].astype('float64' )
        pledge_rate.insert(pledge_rate.shape[1],'pledge_rate',0)
        pledge_rate["pledge_rate"] =  pledge_rate.apply(lambda y:  y['pledge']/y['amount'] , axis=1)
        
        result = pledge_rate.to_json(orient="records")
        parsed = json.loads(result)

        # dis conn
        cur.close()
        conn.close()

        return ApiOutput.success(parsed)




