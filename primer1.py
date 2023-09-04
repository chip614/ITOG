
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Itogovaya rabota").master('local[*]').config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar").getOrCreate()
sc=spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")

'''rdd=sc.parallelize(['Молоко – 80 р.', 'Масло – 140 р.', 'Хлеб – 50 р.', 'Молоко – 94 р.', 'Метро – 50 р.', 'Метро – 50 р.', 'Кафе – 560 р.', 'Кофе – 250 р.', 'Метро – 50 р.'])
rdd1=rdd.map(lambda x: x.split(" "))
rdd2=rdd1.map(lambda x: (x[0],int(x[2])))
print(rdd2.sortByKey().collect())
'''

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://db:5432/db").option("dbtable", "account").option("user","postgres").option("password", "postgres").option("driver", "org.postgresql.Driver").load()

print(df.show(10))



