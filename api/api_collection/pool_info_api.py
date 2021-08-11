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
                


		
	DB_NAME = "cexplorer"
	DB_HOST = "/var/run/postgresql"
	DB_USER = "siri"
	DB_PASS = "PasswordYouWant"
	DB_PORT =  "5432"
	"""
	这里的 input 为 api 入参，类型为 dict。
	"""
	# 连接数据库
	conn = psycopg2.connect(
			            dbname   =   DB_NAME,
			            host     =   DB_HOST,
			            user     =   DB_USER,
			            password =   DB_PASS,
			            port = DB_PORT
			)

	cur = conn.cursor()

	def from_sql_to_df (sql_x):
	    cur.execute(sql_x)
	    x = cur.fetchall()
	    x_df = pd.DataFrame(list(x))
	    return x_df


	#  pool owner的信息
	sql_pool_owner_addr = """
	select epoch_no, pool_hash_id, addr_id from v_pool_owners_by_epoch;
	"""
	pool_owner_addr = from_sql_to_df(sql_pool_owner_addr)
	pool_owner_addr.columns = [ "epoch_no", "pool_hash_id",  "owner_address_id"]
	pool_owner_addr["is_owner"] = 1

	# delegators 的信息
	sql_delegators_info = ''' select delegation.active_epoch_no,stake_address.id, pool_hash.id,epoch_stake.amount
	from delegation 
	inner join stake_address on delegation.addr_id = stake_address.id
	inner join pool_hash on delegation.pool_hash_id = pool_hash.id
	inner join epoch_stake on stake_address.id = epoch_stake.addr_id;
	'''
	delegators_info = from_sql_to_df(sql_delegators_info)
	delegators_info.columns = ['epoch_no',  'addr_id',  'pool_hash_id',   'pool_hash_view',   'delegation_amount']
	delegators_info['is_delegator'] =1


	# 所有人的 stake 地址
	sql_ddr = """
	select id, view from stake_address;
	"""
	addr_relay = from_sql_to_df(sql_ddr)
	addr_relay.columns = [ "addr_id", "stake_address"]

	# epoch+pool 去对应owner和delegator
	owner_delegator = pool_owner_addr.merge(delegators_info, on=['epoch_no','pool_hash_id'], how = 'outer')
	owner_delegator_addr = owner_delegator.merge(addr_relay, on = ['addr_id'], how = 'left')

	# 每个pool在不同epoch产生的block数
	sql_block_pool = """
	select block.epoch_no, pool_hash.id, count (*) as block_count from block 
	inner join slot_leader on block.slot_leader_id = slot_leader.id
	inner join pool_hash on slot_leader.pool_hash_id = pool_hash.id
	group by block.epoch_no, pool_hash.id ;
	"""
	block_pool = from_sql_to_df(sql_block_pool)
	block_pool.columns = [ "epoch_no", "pool_hash_id", "block_amount"]

	#每个pool 在不同 epoch的pledge / amount by epoch
	sql_stake_distribution = "SELECT pool_id, sum (amount), epoch_no FROM epoch_stake GROUP BY pool_id, epoch_no ;"
	stake_distribution = from_sql_to_df(sql_stake_distribution)
	stake_distribution.columns = [ "pool_hash_id","pool_amount", "epoch_no"]

	sql_pledge = 'select epoch_no, pool_hash_id, active_pledge from v_pool_history_by_epoch;'
	pledge = from_sql_to_df(sql_pledge)
	pledge.columns = ['epoch_no','pool_hash_id','pledge']
	

	block_pledge = pledge.merge(block_pool, on=['epoch_no','pool_hash_id'], how = 'outer')
	block_pledge_amount = block_pledge.merge(stake_distribution, on=['epoch_no','pool_hash_id'], how = 'outer')


	# 加上每个pool的地址信息
	sql_pool_id_view = "select id, view from pool_hash;"
	pool_id_view = from_sql_to_df(sql_pool_id_view)
	pool_id_view.columns = ['pool_hash_id','pool_hash_view']
	block_pledge_amount = block_pledge_amount.merge(pool_id_view, on = ['pool_hash_id'], how = 'left')

	print(block_pledge_amount.head())
	# 把所有信息merge在一起
	pool_info = pd.merge(block_pledge_amount,owner_delegator_addr, on = ['epoch_no','pool_hash_id'], how = "outer" )

	result = pool_pledge_0_owner.to_json(orient="records")
	parsed = json.loads(result)


    return ApiOutput.success(parsed)


