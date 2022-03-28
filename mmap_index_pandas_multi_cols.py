import pandas as pd
import sys,sqlite3,time,glob
#
start_time=time.time()
conn1 = sqlite3.connect("index.db")
c1=conn1.cursor()
try:
    c1.execute("create table table_files(table_file_index  INTEGER PRIMARY KEY AUTOINCREMENT, Table_name text, filename text)")
except:
    pass

try:
    c1.execute("create table  table_file_row_positions(table_file_byte_index  INTEGER PRIMARY KEY AUTOINCREMENT, table_file_index int, row_index int,byte_postion_start int, byte_position_end int)")
except:
    pass

try:
    c1.execute("create table  table_file_key(tab_file_key_index  INTEGER PRIMARY KEY AUTOINCREMENT, table_file_index int, key text)")
except:
    pass
try:
    c1.execute("create table tab_file_key_value_index(tab_file_key_value_index  INTEGER PRIMARY KEY AUTOINCREMENT, row_index int, table_file_index int)")
except:
    pass

def index_columns(filename,columnnames,table_file_index,conn1):
   print(table_file_index)
   c1=conn1.cursor()
   adata=[]
   append_cols=''
   append_bind=''

   for columnname in columnnames:
       try:
           table1="select * from table_file_key h1 where h1.table_file_index="+table_file_index+" and h1.columnname='"+str(columnname)+"' "
           a=conn1.execute(table1)
           adata=a.fetchall()
       except:
           pass
       if len(adata)<=0:
          result=c1.execute("insert into table_file_key(table_file_index, key) values('"+str(table_file_index)+"','"+str(columnname)+"')")
          new_row_id=result.lastrowid
       try:
           table1="alter table tab_file_key_value_index add  " + str(columnname)+" text"
           a=conn1.execute(table1)
       except:
           pass
       append_cols=append_cols+columnname+","
       append_bind=append_bind+"?,"

   df_to_write=pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)

   df=pd.read_csv(filename, sep='|')
   #print(df.head(2))

   for columnname in columnnames:
       df_to_write[columnname]=df[columnname].copy()
   #       df_to_write['table_file_index']=new_row_id
   df_to_write['table_file_index']=table_file_index

   df_to_write['ROW_INDEX']=pd.RangeIndex(stop=df_to_write.shape[0])

   print(df_to_write.head())
   big_ind_list=df_to_write.values.tolist()
   sqlite_insert_query = "INSERT INTO tab_file_key_value_index( "+append_cols+"table_file_index, row_index) VALUES ( "+append_bind+"?,?);"
   print(sqlite_insert_query)
   c1.executemany(sqlite_insert_query, big_ind_list)
   conn1.commit()
def create_index_byte_postions(filename,table_file_index,conn1):
    with open(filename, "r+b") as f:
    # memory-map the file, size 0 means whole file
       mm = mmap.mmap(f.fileno(), 0)
       a=mm.find(b'\n', 0,1024)
       ind_list=[]
       big_ind_list=[]
       print(a)
       index_iter=-1
       num_in_list=0
       ind_list.append(table_file_index)
       ind_list.append(index_iter)
       ind_list.append(a)
       #big_ind_list.append(tuple(ind_list))
       ind_list=[]
       b=True
       while b:
              prev_line_end =a
              a=mm.find(b'\n', a+1,a+1+1024)
              #print(a)
              index_iter=index_iter+1
              ind_list.append(table_file_index)
              ind_list.append(index_iter)
              ind_list.append(prev_line_end)
              ind_list.append(a)
              if a==-1:
                 b=False
              num_in_list=num_in_list+1
              big_ind_list.append(tuple(ind_list))
              ind_list=[]
              if num_in_list>10000:
                 sqlite_insert_query = """INSERT INTO table_file_row_positions
                              (table_file_index, row_index,byte_postion_start, byte_position_end)
                              VALUES ( ?, ?, ?,?);"""
                 c1.executemany(sqlite_insert_query, big_ind_list)
                 conn1.commit()
                 big_ind_list=[]
                 num_in_list=0


import mmap
table_name='table1' #dataset name
dir="/"+table_name+"/*.*" # where all the files for a dataset are kept. all of these files should have same record structure.
for filename in glob.glob(dir):
       adata=[]
       try:
           table1="select * from table_files h1 where h1.Table_name='"+str(Table_name)+"' and h1.filename='"+str(filename)+"' "
           a=conn1.execute(table1)
           adata=a.fetchall()
       except:
           pass
       if len(adata)<=0:
          result=c1.execute("insert into table_files(Table_name, filename) values('"+str(table_name)+"','"+str(filename)+"')")

          new_row_id=result.lastrowid
          create_index_byte_postions(filename,new_row_id,conn1)
          index_columns(filename,['col1','col2'],new_row_id,conn1)
       #break
try:
    c1.execute("create index table_file_row_positions_idx01 on table_file_row_positions( table_file_index, row_index)")
except:
    pass
try:
    c1.execute("create index tab_file_key_value_index_idx01 on tab_file_key_value_index( table_file_index, col1 )")
except:
    pass
try:
   c1.execute("create index tab_file_key_value_index_idx02 on tab_file_key_value_index( table_file_index,  col2)")
except:
   pass
