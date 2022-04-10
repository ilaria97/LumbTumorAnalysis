from __future__ import print_function

import sys
import json
from typing import Optional
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
import pyspark
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json, struct, to_json, col
from pyspark.streaming import StreamingContext
import pyspark.sql.types as tp
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from elasticsearch import Elasticsearch
import os

sc = SparkContext(appName="luadAnalysis")
spark = SparkSession(sc)
sc.setLogLevel("WARN")

#training classification model
file="/opt/tap/spark/dataset/luad_clinical.csv"
print("***************** \n Inizio\n")

schema= tp.StructType([
    tp.StructField(name= 'id', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name= 'pathology_report_uuid', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'year_of_diagnosis', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'years_smoked', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'pack_years_smoked', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'age_at_index', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'year_of_birth', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'year_of_death', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'label', dataType= tp.FloatType(),  nullable= True)
])
print(schema)

trainingset= spark.read.csv(file, header=True, schema=schema, sep=',')
trainingset
trainingset.show()

assembler=VectorAssembler(inputCols=['pathology_report_uuid','year_of_diagnosis','years_smoked',
                                    'pack_years_smoked','age_at_index','year_of_birth','year_of_death'],outputCol='features')
regression= LogisticRegression(featuresCol= 'features', labelCol='label')
pipeline=Pipeline(stages=[assembler, regression])
pipelineFit= pipeline.fit(trainingset)
updated_trainingset = pipelineFit.transform(trainingset)
updated_trainingset.show()

print("********************* \n PipelineFit\n")

elastic_host="10.0.100.51"
elastic_index="luad"
elastic_document="_doc"

#mapping for elasticSearch
es_mapping = {
    "mappings": {
        "properties": 
            {
                "id" : {"type": "integer","fielddata": True},
                "@timestamp": {"type": "date","format": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"}
            }
    }
}

def get_spark_session():
    sparkConf= SparkConf() \
                .set("es.nodes", elastic_host) \
                .set("es.port", "9200") \
                .set("spark.app.name", "luadAnalysis") \
                .set("spark.scheduler.mode", "FAIR")
    sc = SparkContext.getOrCreate(conf=sparkConf)


print("************************ \n Es mapping and connection\n")

#Processing data
kafkaServer="10.0.100.23:9092"
topic = "luad"

conf = SparkConf(loadDefaults=False)

dataKafka = tp.StructType([
    tp.StructField(name= 'id', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name= 'pathology_report_uuid', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'year_of_diagnosis', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'years_smoked', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'pack_years_smoked', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'age_at_index', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'year_of_birth', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'year_of_death', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= 'label', dataType= tp.DoubleType(),  nullable= True),
    tp.StructField(name= '@timestamp', dataType= tp.StringType(),  nullable= True)
])

print("************************ \n dataKafka\n")

def elaborate(batch_df: DataFrame, batch_id: int):
    batch_df.show(truncate=False)
    if not batch_df.rdd.isEmpty():
        print("******************** \n elaborate\n")
        batch_df.show()                
        data2=pipelineFit.transform(batch_df)
        data2.show()
        data2.summary()

        print("************************ \nSend to ES \n")
        data2.select("id", "@timestamp", "year_of_diagnosis", "years_smoked", "pack_years_smoked", "age_at_index", "year_of_birth", "year_of_death", "prediction", "label") \
        .write \
        .format("org.elasticsearch.spark.sql") \
        .mode('append') \
        .option("es.mapping.id","id") \
        .option("es.nodes", elastic_host).save(elastic_index)

print("************************ \n Creating a kafka source\n")
#creating a kafka source
#streaming query
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafkaServer) \
  .option("subscribe", topic) \
  .load()

print("************************ \nTaking data from kafka\n")
#batch queries
df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", dataKafka).alias("data")) \
    .select("data.*") \
    .writeStream \
    .foreachBatch(elaborate) \
    .start() \
    .awaitTermination()

print("************************ \n kafka server\n")