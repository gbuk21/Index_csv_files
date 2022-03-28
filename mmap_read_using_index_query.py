import sqlite3,os,time,mmap,re
import pandas as pd
start_time=time.time()
line_in=''

conn1 = sqlite3.connect('/index1.db') #change to the directory where sqlite db that holds index data 

def split_query(condition,key_list):
   import nltk

   patterns = [
    {r'.*SELECT$', 'SELECT'},
    {r'.*INSERT$', 'INSERT'},
    {r'.*UPDATE$', 'UPDATE'},
    {r'.*DELETE$', 'DELETE'},
    {r'.*ORDER', 'ORDER'},
     {r'.*AND', 'AND'},
     {r'.*OR', 'OR'},
     {r'.*FROM', 'FROM'},
     {r'.*WHERE', 'WHERE'},
      {r'.*,', 'COMMA'},
      {r'.*\(', 'LEFT'},
      {r'.*\)', 'RIGHT'},
      {r'.*NOT', 'NOT'},
      {r'.*IN', 'IN'},
      {r'.*>=', 'GE'},
      {r'.*==', 'EQ'},
     {'.*', 'NN'} ]


   regexp_tagger = nltk.RegexpTagger(patterns)
   word_list=nltk.word_tokenize(condition)
   print(word_list)
   parent_list=[]
   child_list=[]
   appended_query=''
   current_level=0
   parent_level=-1
   tag_order=0
   #key_list=['STORE_NUM']
   for word in word_list:
      if word.strip()=='':
         continue
      if word.upper()=='(':
         parent_level=parent_level+1
         current_level =current_level+1
         tag_order=tag_order+1
         parent_list.append([word, parent_level,current_level,tag_order ])
      elif word.upper()==')':
         parent_level=parent_level-1
         current_level =current_level-1
         tag_order=tag_order+1
         if appended_query != '':
            child_list.append([appended_query, parent_level,current_level,tag_order ])
         parent_list.append([word, parent_level,current_level,tag_order ])
         appended_query=''
      elif word.upper()=='AND' or word.upper()=='OR':
         parent_level=parent_level-1
         current_level =current_level-1
         tag_order=tag_order+1
         if appended_query != '':
            child_list.append([appended_query, parent_level+1,current_level+1,tag_order-1 ])
         parent_list.append([word , parent_level,current_level,tag_order ])
         appended_query=''
      else:
        tag_order=tag_order+1
        if word=="'":
           appended_query=appended_query+"" + word
        else:
            appended_query=appended_query+" " + word
      #print(child_list)
      #print(appended_query)
   child_list.append([appended_query, parent_level+1,current_level+1,tag_order-1 ])
   print('child_list')
   print(child_list)
   print(parent_list)
   child_list1=[]
   child_list2=[]
   for i  in key_list:
       for j in child_list:
           print(i)
           print(j)
           if i[0] in j[0]:
              j.append(1)
              child_list1.append(j)
   print('child_list1')
   print(child_list1)

   for i in range(tag_order+1):
       #print(i)
       processed=True
       for  j in child_list1:
            #print(j)
            if i ==j[3]:
               j.append(1)
               child_list2.append(j)

               processed=False
       for  j in child_list:
            #print(j)
            if i ==j[3] and processed:
               j.append(0)
               j.append(0)
               child_list2.append(j)
               #processed=False
       for  j in parent_list:
            #print(j)
            if i ==j[3]: #and processed:
               j.append(0)
               j.append(1)
               child_list2.append(j)
               processed=False
   print(child_list2)
   query_to_be_used=''
   prev_or=False
   prev_and=False
   for i in child_list2:
       print(i[5])
       print(i)


       if 'AND' == str(i[0]).upper().strip():
          prev_and=True
       if 'OR' == str(i[0]).upper().strip():
          prev_or=True
       if prev_or and i[5]==0 and i[0]!='':
           query_to_be_used=query_to_be_used+'  1=2 '
           prev_or=False
       elif prev_and and i[5]==0 and i[0]!='':
           query_to_be_used=query_to_be_used+' 1=1 '
           prev_and=False
       elif i[0]=="'":
            query_to_be_used=query_to_be_used+''+i[0]
       else: query_to_be_used=query_to_be_used+' '+i[0]
   query_to_be_used=query_to_be_used.replace("[","(").replace("]",")").replace("==","=")
   return query_to_be_used

def get_data(tablename,condition):
   if 1==1:
        conn1 = sqlite3.connect('/index1.db')
        c1=conn1.cursor()
        c1.execute("select distinct  key from table_files tf, table_file_key tfk where  tf.table_file_index =tfk.table_file_index and  tf.Table_name='"+str(tablename)+"'")
        b=c1.fetchall()
        split=split_query(condition,b)
        print(split)
        if len(b)>=0:
           return_list=[]
           for i in b:
               if i[0] in condition:
                  if 1==1: #for split_loop in split:
                      #print('split_loop')
                      #print(i[1])
                      #print(split_loop)
                      if 1==1:  # if str(i[1]) in split_loop[0]:
                         print("select filename, byte_postion_start,byte_position_end from table_files tf, table_file_key tfk , table_file_row_positions tfr , tab_file_key_value_index tfkv where  tf.Table_name='"+str(tablename)+"' and "+str(split)+" and  tfr.table_file_index= tf.table_file_index and tfk.table_file_index= tf.table_file_index  and tfk.table_file_index= tf.table_file_index and tfk.table_file_index= tfkv.table_file_index  and tfkv.row_index=tfr.row_index  order by 1,2")
                         c1.execute("select  distinct filename, byte_postion_start,byte_position_end from ( select filename,tf.table_file_index, row_index from table_files tf,  tab_file_key_value_index tfkv where  tf.Table_name='"+str(tablename)+"'  and    tf.table_file_index= tfkv.table_file_index and "+str(split)+"  ) t, table_file_row_positions tfr  where t.table_file_index= tfr.table_file_index and t.row_index=tfr.row_index ")

                         b1=c1.fetchall()
                         print('length')
                         print(len(b1))
                         prev_file_name=''
                         for i1 in b1:
                             if i1[0]!=prev_file_name:
                                 f= open(i1[0], "r+b")
                                 mm = mmap.mmap(f.fileno(), 0)
                                 header=mm.readline()
                                 header=str(header)[2:-3].split("|")
                                 #print(header[:-1])
                             prev_file_name =i1[0]
                             line1=mm[int(i1[1])+2:int(i1[2])]
                             return_list.append(str(line1)[2:-3].split("|"))
                             #print(str(line1)[2:-3].split("|"))
                  df_to_return = pd.DataFrame(return_list, columns=header[:-1])
                  #print(condition)
                  #print(df_to_return.columns)
                  #print(df_to_return['SKU'])
                  df_to_return=df_to_return.query(condition,inplace=False)
                  df_to_return.to_csv("output.csv",sep="|",mode='w')
                  print(df_to_return.head())
                  #conn1.close()
                  break
        else:
             c1.execute("select distinct filename from table_files tf where  tf.Table_name='"+str(tablename)+"'")
             b=c1.fetchall()
             df_concat=pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)
             for i in b:
                print('entire df')
                print(i)
                df=pd.read_csv(i[0], sep="|")
                df_selected=df.query(condition,inplace=False)
                if len(df_selected):
                   if len(df_concat)<=0:
                      df_concat=df_selected
                   else:
                      frames = [df_concat, df_selected]
                      df_concat=pd.concat(frames,sort=False)
             df_concat.to_csv("output.extract.csv",sep="|",mode='w')
   conn1.close()

get_data("dw5", "(col1 in [ '19001','19002','19003'] and col2=='00010006')")
