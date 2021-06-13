from pyspark.sql.functions import *
from pyspark.sql.window import Window
from subprocess import PIPE, Popen
import threading

sh_proc = Popen(("hdfs dfs -ls /tmp/test_words/").split(" "), stdout = PIPE,universal_newlines=True)
vs_out, vs_err = sh_proc.communicate()

file_list=[]

for i in vs_out.split('\n'):
	if i.startswith('-'):
		file_list.append(i.split('/')[-1])

df_final=[]
df_final[:]=[]

threads = []

def dict_calulation(i):
	input_df = spark.read.format("text").load("/tmp/test_words/"+i).withColumn("filepath", input_file_name())
	input_df = input_df.withColumn("docID",reverse(split(input_df["filepath"],'/'))[0]).drop("filepath")
	clean_df = input_df.withColumn("clean_sentence",regexp_replace(lower(input_df["value"]),"[^a-zA-Z0-9\ ]","" ) ).drop("value")
	split_df = clean_df.withColumn("split_word",split(clean_df["clean_sentence"],' ')).drop("clean_sentence")
	explode_df = split_df.withColumn("wordID",explode(split_df["split_word"])).drop("split_word").filter("wordID != ''").select("wordID","docID").distinct()
	explode_df.cache()
	df_final.append(explode_df)

for g in file_list:
	t = threading.Thread(target=dict_calulation, args=(g,))
	threads.append(t)
	t.start()
	if (threading.active_count() > 10):
		t.join()

for one_thread in threads:
	one_thread.join()

j=0
for i in df_final:
	if(j==0):
		union_df=i
		j=j+1
	else:
		union_df=union_df.union(i)

merge_df = union_df.withColumn("docIDList",collect_list(union_df["docID"]).over(Window.partitionBy(union_df["wordID"]).orderBy(union_df["docID"])) )
final_df = merge_df.groupBy("wordID").agg(max("docIDList")).orderBy("wordID")

print("final_df count: "+str(final_df.count()))