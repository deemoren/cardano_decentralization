from api.base.base_api import BaseApi
from api.base.dto.api_output import ApiOutput
import psycopg2

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

    # The basic information of Cardano：epoch number, slot number , and block number
        #  latest epoch numner
        sql = " select MAX(epoch_no) from block;"
        cur.execute(sql)
        last_epoch  = cur.fetchall()
        last_epoch = str(last_epoch)
        last_epoch = last_epoch[2:-3]
        most_recent_epoch = int(last_epoch) 
        
        # latest slot number
        sql_most_recent_slot = "select slot_no from block where block_no is not null order by block_no desc limit 1;"
        cur.execute(sql_most_recent_slot)
        most_recent_slot = cur.fetchall()
        most_recent_slot = str(most_recent_slot)
        most_recent_slot = most_recent_slot[2:-3]

        # latest block number
        sql_most_recent_block = "select MAX(block_no) from block where block_no is not null ;"
        cur.execute(sql_most_recent_block)
        most_recent_block = cur.fetchall()
        most_recent_block = str(most_recent_block)
        most_recent_block = int(most_recent_block[2:-3])

        x = "The most recent epoch is", most_recent_epoch, "The most recent slot is", most_recent_slot, "The most recent block is", most_recent_block
        cur.close()
        conn.close()


        # 访问数据库的操作封装起来，一处定义，多处使用
        # db.query_as_pd("SELECT * from table")
        # db.query("SELECT * from table")

        return ApiOutput.success(str(x))
