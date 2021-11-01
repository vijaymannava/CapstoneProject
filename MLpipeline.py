import pickle
import pymongo
import json
import pyspark
from pyspark import SparkContext 
from pyspark import conf
from pyspark.sql import SparkSession, udf
from pyspark.sql import SQLContext
import string
import re
import pyspark.ml.feature
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lower,col , udf,countDistinct
from pyspark.ml.feature import Tokenizer,RegexTokenizer,StopWordsRemover,StringIndexer
from pyspark.ml.feature import NGram,HashingTF,IDF ,CountVectorizer,VectorAssembler
from pyspark.ml.classification import LogisticRegression,NaiveBayes,RandomForestClassifier,GBTClassifier
from pyspark.ml.evaluation import Evaluator, MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.ml.param import *
import pandas



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
#df.select("summary","title","category").show(5)
df=df.select("summary","category")
#df.toPandas()['category'].isnull().sum()

#print(df.printSchema())
#print(df.first())


# Removing "news","nation" category - 
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
 #textclean(col("title")).alias("title")
 textclean(col("summary")).alias("summary"))

#nonews = df.count()
#print("before",rowcount)
#print("after",nonews)
#df.show(5)
#dir(pyspark.ml.feature)
#print(dir(pyspark.ml.feature))

#Value counts
df_countacat=df.groupBy("category").count()
#df_countacat.show()


# Data Cleaning
# Handling null values-droping null records
#rowcount=df.count()
#print(rowcount)
#nul_drop = df.dropna()
#rowcount_null = nul_drop.count()
#print("before",rowcount)
#print("after",rowcount_null)
df=df.dropna(subset=("category"))
#df.show()


#Feature Extraction

#Build Features From Text
#    CountVectorizer
#    TFIDF
#    WordEmbedding
#    HashingTF

#dir(pyspark.ml.feature)
#print(dir(pyspark.ml.feature))

# Stages For the Pipeline
tokenizer = Tokenizer(inputCol='summary',outputCol='mytokens')
stopwords_remover = StopWordsRemover(inputCol='mytokens',outputCol='filtered_tokens')
vectorizer = CountVectorizer(inputCol='filtered_tokens',outputCol='rawFeatures')
idf = IDF(inputCol='rawFeatures',outputCol='vectorizedFeatures')

# LabelEncoding/LabelIndexing
labelEncoder = StringIndexer(inputCol='category',outputCol='label').fit(df)


#labelEncoder.transform(df).show(5)

# Dict of Labels
label_dict = {'science':0.0, 'business':1.0,'economics':2.0,'finance':3.0,'tech':4.0,
'gaming':5.0, 'entertainment':6.0, 'sport':7.0,'beauty':8.0, 'politics':9.0, 
'world':10.0, 'energy':11.0, 'food':12.0, 'travel':13.0}

#df.show()

df = labelEncoder.transform(df)

#df.show()

### Split Dataset
(trainDF,testDF) = df.randomSplit((0.7,0.3),seed=42)

trainDF.show(5)
testDF.show(5)

