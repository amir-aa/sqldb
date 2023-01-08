import mysql.connector

OVERAL=20#number of cycles/
CYCLESIZE=2000000#number of records in each cycle(divided table)
USER='user'
PASSWORD='Pass'
DB='db'
SERVER='Server.local'
MAINTABLE='tbl'
mydb = mysql.connector.connect(
  host=SERVER,
  user=USER,
  password=PASSWORD,
  database=DB
)
for cycle in range(1,OVERAL):
    tblname=f"table_{cycle}"
    create_table_query=f"create table IF NOT EXISTS {tblname}(id int auto_increment ,val varchar(200),PRIMARY KEY(id))"
    cursor=mydb.cursor(buffered=True)
    cursor.execute(create_table_query)
    

    cursor2=mydb.cursor(buffered=True)
    N=(cycle*CYCLESIZE)+1
    
    
    cursor.execute(f'select * from {MAINTABLE} where id<{N} and id>{(cycle-1)*CYCLESIZE}')
    query="insert into {}(val) VALUES(%s)".format(tblname)
    cursor2.executemany(query,tuple((str(item[1]),) for item in cursor))
    mydb.commit()

    print(f"{CYCLESIZE} records inserted") 
    print(f"Cycle {cycle} Inserted")
    