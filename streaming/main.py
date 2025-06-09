import requests 
import pyspark
import pandas as pd
import os
import time


# JAVA 11
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# SPARK
os.environ["SPARK_HOME"] = "/opt/spark-3.5.6-bin-hadoop3"

# PYSPARK com Python 3.11
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["PYSPARK_PYTHON"] = "python3"

# HADOOP
os.environ["HADOOP_HOME"] = "/opt/spark-3.5.6-bin-hadoop3"


# Dados de conexão
jdbc_url = os.getenv('jdbc_url')
tabela = "analise_vendas_spark"

user = os.getenv('user')
password = os.getenv('password')
driver = "org.postgresql.Driver"


spark = pyspark.sql.SparkSession.builder                     \
    .appName('Streaming')                                     \
    .config("spark.jars", "/home/ubuntu/libs/postgresql-42.7.3.jar") \
    .getOrCreate()

print(spark.sparkContext._conf.get("spark.jars"))

schema = "id_pedido STRING, timestamp TIMESTAMP, id_produto STRING, nome_produto STRING, categoria STRING, preco DOUBLE, quantidade INTEGER"


df_stream = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load('~/analise_vendas_spark/streaming/dados')



print('Preparando para inserir no banco...')

def write_to_db(batch_df,batch_id):
    batch_df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", tabela) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode("append") \
        .save()
    
diretorio = '~/analise_vendas_spark/streaming/diretorio_de_controle'

strcal = df_stream.writeStream.foreachBatch(write_to_db).outputMode("append") \
    .trigger(processingTime="10 second") \
    .option("checkpointLocation", diretorio) \
    .start() 

strcal.awaitTermination()


