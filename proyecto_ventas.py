import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum, avg, count, trim, when, to_date, year

ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DATOS", "pdi_sales.csv")

spark = SparkSession.builder.appName("Proyecto-Ventas").master("local[*]").config("spark.ui.showConsoleProgress", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def titulo(texto):
    print("\n" + "=" * 60)
    print(texto)
    print("=" * 60)


titulo("1. Carga del CSV")
df = spark.read.csv(ruta, header=True, inferSchema=True, sep=";")
df = df.withColumn("Zip", trim(col("Zip"))).withColumn("Country", trim(col("Country")))

titulo("2. Esquema y número de filas")
df.printSchema()
print(f"Total de filas: {df.count()}")

titulo("3. Ingresos totales por país")
df.groupBy("Country").agg(sum("Revenue").alias("Ingresos_Totales")).orderBy(desc("Ingresos_Totales")).show()

titulo("4. Ventas de Alemania con más de 1 unidad")
alemania = df.filter((col("Country") == "Germany") & (col("Units") > 1))
alemania.show(10)
print(f"Ventas encontradas: {alemania.count()}")

titulo("5. Código postal con más ventas en Alemania")
(
    df.filter(col("Country") == "Germany")
    .groupBy("Zip")
    .agg(count("*").alias("Num_Ventas"))
    .orderBy(desc("Num_Ventas"))
    .show(5)
)

titulo("6. Clasificación de ventas")
df_clasificado = df.withColumn("Clasificacion", when(col("Revenue") > 500, "Alta").otherwise("Normal"))
df_clasificado.show(10)
df_clasificado.groupBy("Clasificacion").count().show()

titulo("7. (Bonus) Media de ingresos por año")
df_anio = df.withColumn("Anio", year(to_date(col("Date"), "M/d/yyyy")))
df_anio.groupBy("Anio").agg(avg("Revenue").alias("Media_Ingresos")).orderBy("Anio").show(30)

spark.stop()
