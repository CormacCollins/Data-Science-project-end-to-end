import pyodbc
import pandas as pd

# tables
ling_score = r'C:\Users\corma\Documents\SQL study\\final tables\Linguistic_score_table.csv'
speaker_occs = r"C:\Users\corma\Documents\SQL study\\final tables\speaker_occs.csv"
tags = r"C:\Users\corma\Documents\SQL study\\final tables\\tags.csv"
talk_fact_dim = r"C:\Users\corma\Documents\SQL study\\final tables\\talk_fact_dim.csv"
talk_meta_data_dim = r"C:\Users\corma\Documents\SQL study\\final tables\\talk_meta_data_dim.csv"
Ted_talks_related = r"C:\Users\corma\Documents\SQL study\\final tables\\Ted_talks_related.csv"
user_ratings = r"C:\Users\corma\Documents\SQL study\\final tables\\user_ratings.csv"

df_ling = pd.read_csv(ling_score, index_col = False)
df_occs = pd.read_csv(speaker_occs, index_col = False)
df_tags = pd.read_csv(tags, index_col = False)
df_fact_dim = pd.read_csv(talk_fact_dim, index_col = False)
df_meta_dim = pd.read_csv(talk_meta_data_dim, index_col = False)
df_related = pd.read_csv(Ted_talks_related, index_col = False)
df_ratings = pd.read_csv(user_ratings, index_col = False)

#df_ling.drop(columns=['Index'])
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'localhost\SQLEXPRESS' 
database = 'Ted talk' 
username = 'LAPTOP-9UD7NL01\corma' 
password = '' 
cnxn = pyodbc.connect(Trusted_Connection='yes', DRIVER='{SQL Server}', SERVER=server, DATABASE=database, UID=username)
cursor = cnxn.cursor()

def define_sql_insert(table, into_cols):
    return "INSERT INTO " + table + " (" \
    + ",".join(into_cols) + ")" \
    + " values(" + "?" + ",?" * (len(into_cols)-1) + ")"

def rows_exist(table, curs, d_frame):
    qry =  "select count(*) from " + table
    cursor.execute(qry)
    res = cursor.fetchall()[0]
    #print(res[0])
    cursor.commit()
    return res[0] != 0

print("Writing db tables")  
#print(rows_exist("dbo.Linguistic_score_table", cursor, df_ling))
#exit()
#linquistics
if not rows_exist("dbo.Linguistic_score_table", cursor, df_ling):
    sql_linq = define_sql_insert("dbo.Linguistic_score_table", df_ling.columns)  
    for index, row in df_ling.iterrows():
        cursor.execute(sql_linq, tuple(row.tolist()))
    print("Linguistic_score_table db written")
else:
    print("dbo.Linguistic_score_table already full")


# Occupation
if not rows_exist("dbo.Speaker", cursor, df_occs):    
    sql_occs = define_sql_insert("dbo.Speaker", df_occs.columns)  
    for index, row in df_occs.iterrows():
        cursor.execute(sql_occs, tuple(row.tolist()))
    print("Speaker db written")
else:
    print("dbo.Speaker already full")


#fact
if not rows_exist("dbo.Talk_fact_dim", cursor, df_fact_dim):
    sql_fact = define_sql_insert("dbo.Talk_fact_dim", df_fact_dim.columns)  
    for index, row in df_fact_dim.iterrows():
        cursor.execute(sql_fact, tuple(row.tolist()))
    print("Talk_fact_dim db written")
else:
    print("dbo.Talk_fact_dim already full")

#tags
if not rows_exist("dbo.Tag_table", cursor, df_tags):
    sql_tags = define_sql_insert("dbo.Tag_table", df_tags.columns)  
    for index, row in df_tags.iterrows():
        cursor.execute(sql_tags, tuple(row.tolist()))
    print("Tags db written")
else:
    print("dbo.Tag_table already full")


#meta data
if not rows_exist("dbo.Talk_meta_data_dim", cursor, df_meta_dim):
    sql_meta = define_sql_insert("dbo.Talk_meta_data_dim", df_meta_dim.columns)  
    for index, row in df_meta_dim.iterrows():
        cursor.execute(sql_meta, tuple(row.tolist()))
    print("Talk_meta_data_dim db written")
else:
    print("Talk_meta_data_dim already full")
#related
if not rows_exist("dbo.Related_talks_id", cursor, df_related):
    sql_related = define_sql_insert("dbo.Related_talks_id", df_related.columns)   
    for index, row in df_related.iterrows():
        cursor.execute(sql_related, tuple(row.tolist()))
    print("Related_talks_id db written")
else:
    print("Related_talks_id already full")

#ratings
if not rows_exist("dbo.User_ratings", cursor, df_ratings):     
    sql_ratings = define_sql_insert("dbo.User_ratings", df_ratings.columns) 
    for index, row in df_ratings.iterrows():
        cursor.execute(sql_ratings, tuple(row.tolist()))
    print("User_ratings db written")
else:
    print("User_ratings already full")

cnxn.commit()
cursor.close()
print("db tables commited")