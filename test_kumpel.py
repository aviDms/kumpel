from postgresql import Database
from config_data.db_conn_parameters import dwh
import time


start = time.time()
db = Database(uri=dwh[''][''], schema='dwh_dl')

end = time.time()

print(db.bids.list_columns())


for c in db.bids.get_constraints():
    print(c)

print(end-start)