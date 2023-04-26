from pyspark.sql import SparkSession
from pyspark.sql import Window, DataFrame, functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *

##Configuração
BUCKET = 'datlo-companies'
spark = SparkSession.builder.appName("Read CSV from S3").getOrCreate()
spark.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set('spark.sql.session.timeZone', 'America/Sao_Paulo')


##Leitura dos arquivos csv
df = spark.read.csv(f"s3a://{BUCKET}/companies/*.csv", header=False, inferSchema=True, sep=';')


##Reestruturação e renomeação das colunas
df = df.withColumnRenamed("_c0","cnpj") \
        .withColumnRenamed("_c1","razao_social") \
        .withColumnRenamed("_c2","nat_juridica") \
        .withColumnRenamed("_c3","qualif_resp") \
        .withColumnRenamed("_c4","cap_social_empresa") \
        .withColumnRenamed("_c5","porte_empresa_id") \
        .withColumnRenamed("_c6","ente_federativo_resp") \
        .withColumn("new_gender", when(col("porte_empresa_id") == 0,"NÃO INFORMADO")
              .when(col("porte_empresa_id") == 1,"MICRO EMPRESA")
              .when(col("porte_empresa_id") == 3,"EMPRESA DE PEQUENO PORTE")
              .otherwise("DEMAIS"))

##Salvando com particionamento em parquet
S3_URL = f"s3a://{BUCKET}/companies/final_parquet/empresasFinal.parquet"
df.write.format("parquet").option("header", "true").mode("overwrite").save(S3_URL)


##Salvando em um arquivo único como arquivo csv
S3_URL = f"s3a://{BUCKET}/companies/final_csv/empresasFinal.csv"
df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(S3_URL)
