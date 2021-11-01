import pymongo
import json
import pyspark
from pyspark import SparkContext
from pyspark import conf
from pyspark.sql import SparkSession, udf
from pyspark.sql import SQLContext
import string
import re
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lower,col , udf,countDistinct
from pyspark.ml.feature import Tokenizer,RegexTokenizer,StopWordsRemover,StringIndexer
from pyspark.ml.feature import NGram,HashingTF,IDF ,CountVectorizer,VectorAssembler
from pyspark.ml.classification import LogisticRegression,NaiveBayes,RandomForestClassifier,GBTClassifier
from pyspark.ml.evaluation import Evaluator, MulticlassClassificationEvaluator
from pyspark.sql.types import IntegerType
from pyspark.ml.param import *

# load mongo data
input_uri = "mongodb://localhost:27017/capstone.news"
output_uri = "mongodb://localhost:27017/capstone.news"

myspark = SparkSession\
    .builder\
    .appName("mynews")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()

df = myspark.read.format('com.mongodb.spark.sql.DefaultSource').load()

#print(df.printSchema())
#print(df.first())

# Data Cleaning
# Handling null values-droping null records
rowcount=df.count()
 #print(rowcount)
 #nul_drop = df.dropna()
 #rowcount_null = nul_drop.count()
 #print("before",rowcount)
 #print("after",rowcount_null)
df=df.dropna()

# Removing "news" category - 
df.createOrReplaceTempView("capstone_news")
sql_query = "select * from capstone_news where category NOT IN ('news','nation') "
df=myspark.sql(sql_query)

#Convert to lowercase
def textclean(text):
    text = lower(text)
   # text = regexp_replace(text,"\+[.*?\]","")
    text = regexp_replace(text,"^rt","")
   # text = regexp_replace(text,"\w*\d\w*","")
    text = regexp_replace(text,"[0-9]","")
    text = regexp_replace(text,"(https?\://)\S+","")
    text = regexp_replace(text,'\[.*?\]', '')
    text = regexp_replace(text,'/', '')
    text = regexp_replace(text,':', '')
    text = regexp_replace(text,'%', '')
    text = regexp_replace(text,'\n', '')
    #text = regexp_replace(text,'-,', '')
    
    return text
df = df.select(textclean(col("category")).alias("category"),
 textclean(col("title")).alias("title")
,textclean(col("summary")).alias("summary"))

df.write.format("mongo").mode("append").option("database","capstone").option("collection", "news_csv").save()