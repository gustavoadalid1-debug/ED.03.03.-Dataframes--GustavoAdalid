from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, desc, avg, max, min, sum, count, rank, trim, concat_ws

spark = SparkSession.builder.appName("Ejercicios-DataFrames").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def titulo(texto):
    print("\n" + "=" * 60)
    print(texto)
    print("=" * 60)


titulo("Ejercicio 1: Creación y Exploración")
datos = [
    ("Ana", 28, "Ventas", 35000),
    ("Juan", 35, "IT", 45000),
    ("María", 42, "IT", 55000),
    ("Pedro", 31, "Ventas", 38000),
    ("Laura", 29, "Marketing", 40000),
]
df = spark.createDataFrame(datos, ["nombre", "edad", "departamento", "salario"])
df.printSchema()
df.show(3)
print(f"Total de empleados: {df.count()}")


titulo("Ejercicio 2: Filtrado")
print("IT con salario > 40000")
df.filter((col("departamento") == "IT") & (col("salario") > 40000)).show()

print("Ventas o Marketing")
df.filter((col("departamento") == "Ventas") | (col("departamento") == "Marketing")).show()

print("No son de IT")
df.filter(~(col("departamento") == "IT")).show()

print("Entre 30 y 40 años")
df.filter((col("edad") >= 30) & (col("edad") <= 40)).show()


titulo("Ejercicio 3: Manipulación de Columnas")
df3 = df.withColumn("salario_en_5_años", col("salario") + (col("salario") * 0.05 * 5))
df3 = df3.withColumnRenamed("nombre", "empleado")
df3.show()


titulo("Ejercicio 4: Agregaciones")
print("Salario promedio por departamento")
df.groupBy("departamento").agg(avg("salario").alias("salario_promedio")).show()

print("Salario máximo y mínimo por departamento")
df.groupBy("departamento").agg(
    max("salario").alias("salario_maximo"),
    min("salario").alias("salario_minimo"),
).show()

print("Empleados por departamento")
df.groupBy("departamento").agg(count("*").alias("total_empleados")).show()

print("Gasto total en salarios por departamento")
df.groupBy("departamento").agg(sum("salario").alias("gasto_total")).orderBy(desc("gasto_total")).show()


titulo("Ejercicio 5: Window Functions")
ventas = spark.createDataFrame(
    [("Enero", "ProdA", 100), ("Enero", "ProdB", 150), ("Febrero", "ProdA", 120)],
    ["mes", "producto", "ventas"],
)
ventana = Window.partitionBy("mes").orderBy(col("ventas").desc())
ventas.withColumn("ranking", rank().over(ventana)).show()


titulo("Ejercicio 6: Joins")
personas = spark.createDataFrame([("Ana", 1), ("Luis", 2), ("Marta", 3)], ["Nombre", "DepID"])
departamentos = spark.createDataFrame([(1, "IT"), (2, "HR")], ["DepID", "NombreDep"])
personas.join(departamentos, "DepID", "left").select("Nombre", "DepID", "NombreDep").show()
print("Marta tiene DepID 3, que no existe en departamentos: el left join la conserva con NombreDep = null.")


titulo("Ejercicio 7: SQL Puro")
df.createOrReplaceTempView("empleados")
spark.sql("""
    SELECT *
    FROM empleados
    WHERE departamento = 'IT' AND salario > 40000
""").show()


titulo("Ejercicio 8: Funciones de String")
nombres = spark.createDataFrame([(" Juan ", "Garcia"), ("Ana", " Pérez ")], ["nombre", "apellido"])
limpio = nombres.withColumn("nombre", trim(col("nombre"))).withColumn("apellido", trim(col("apellido")))
limpio.withColumn("Nombre_Completo", concat_ws(" ", col("nombre"), col("apellido"))).show()

spark.stop()
