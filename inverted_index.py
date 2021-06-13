from pyspark.sql.functions import *
from pyspark.sql.window import Window

input_df = spark.read.format("text").load("/tmp/test_words/").withColumn("filepath", input_file_name())
input_df = input_df.withColumn("docID",reverse(split(input_df["filepath"],'/'))[0]).drop("filepath")

input_df.cache()
input_df.count()

clean_df = input_df.withColumn("clean_sentence",regexp_replace(lower(input_df["value"]),"[^a-zA-Z0-9\ ]","" ) ).drop("value")

split_df = clean_df.withColumn("split_word",split(clean_df["clean_sentence"],' ')).drop("clean_sentence")
explode_df = split_df.withColumn("wordID",explode(split_df["split_word"])).drop("split_word").filter("wordID != ''").select("wordID","docID").distinct()

merge_df = explode_df.withColumn("docIDList",collect_list(explode_df["docID"]).over(Window.partitionBy(explode_df["wordID"]).orderBy(explode_df["docID"])) )

final_df = merge_df.groupBy("wordID").agg(max("docIDList")).orderBy("wordID")

print("final_df count: "+str(final_df.count()))