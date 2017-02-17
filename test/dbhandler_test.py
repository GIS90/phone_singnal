# -*- coding: utf-8 -*-

"""
------------------------------------------------
describe: 
------------------------------------------------
"""

host = "localhost"
port = 1433
user = "sa"
password = "123456"
database = "qd_db"

dbhandle = DBHandler(host=host,
                     port=port,
                     user=user,
                     password=password,
                     database=database)
conn = dbhandle.open()
print conn
query_sql = "select * from cell_jtxq"
rlt = dbhandle.query(query_sql, 2)
for row in rlt:
    print row
dbhandle.close()



