import time
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from common import get_spark, BASE
from schemas import order_schema, resto_schema

spark = get_spark("p3_sql")
spark.sparkContext.setLogLevel("WARN")


def chrono(label, fn):
    t = time.time()
    res = fn()
    print(f"[TEMPS] {label}: {time.time() - t:.2f} s")
    return res


# 1. Lecture avec schéma explicite (aucune inférence)
orders = spark.read.schema(order_schema).json(f"{BASE}/bronze/history/orders_history.jsonl")
restos = (spark.read.schema(resto_schema).option("multiLine", True)
          .json(f"{BASE}/bronze/restaurants.json"))
print("Commandes :", orders.count(), "| Restaurants :", restos.count())
orders.printSchema()

deliv = orders.filter("status = 'DELIVERED'")

# 2a. CA par ville : API DataFrame
print("\n=== CA par ville (DataFrame) ===")
chrono("DataFrame CA par ville",
       lambda: deliv.groupBy("city").agg(F.round(F.sum("amount_mad"), 2).alias("ca"))
       .orderBy("city").show())

# 2b. CA par ville : SQL
orders.createOrReplaceTempView("orders")
print("=== CA par ville (SQL) ===")
chrono("SQL CA par ville",
       lambda: spark.sql("""SELECT city, ROUND(SUM(amount_mad),2) AS ca
                            FROM orders WHERE status = 'DELIVERED'
                            GROUP BY city ORDER BY city""").show())

# 3. Jointure avec le référentiel (on ne garde que la catégorie et le nom,
#    la colonne city est conservée depuis orders)
restos_s = restos.select("restaurant_id", "category", F.col("name").alias("resto_name"))
j = deliv.join(restos_s, "restaurant_id")
print("=== CA par catégorie et par ville ===")
j.groupBy("city", "category").agg(F.round(F.sum("amount_mad"), 2).alias("ca")) \
 .orderBy("city", F.desc("ca")).show(30)

# 4. UDF vs fonctions natives
@udf("string")
def distance_band(d):
    if d is None:
        return None
    return "court" if d < 2 else ("moyen" if d <= 5 else "long")


band_native = (F.when(F.col("distance_km") < 2, "court")
               .when(F.col("distance_km") <= 5, "moyen")
               .otherwise("long"))

print("=== distance_band : UDF ===")
chrono("UDF", lambda: j.withColumn("band", distance_band("distance_km"))
       .groupBy("band").count().orderBy("band").show())
print("=== distance_band : when/otherwise ===")
chrono("natif when/otherwise", lambda: j.withColumn("band", band_native)
       .groupBy("band").count().orderBy("band").show())

# 5. Silver : DELIVERED, dédoublonné, enrichi, partitionné par ville
n_avant = deliv.count()
clean = (j.dropDuplicates(["order_id", "status"])
         .withColumn("distance_band", band_native))
n_apres = clean.count()
print(f"DELIVERED avant dédoublonnage : {n_avant} | après : {n_apres} "
      f"({n_avant - n_apres} doublons supprimés)")

chrono("Ecriture Silver Parquet",
       lambda: clean.write.mode("overwrite").partitionBy("city")
       .parquet(f"{BASE}/silver/orders_history/"))
print("Silver écrit dans", f"{BASE}/silver/orders_history/")

# 6. explain(True) sur la jointure
print("\n=== explain(True) ===")
j.groupBy("city", "category").agg(F.sum("amount_mad").alias("ca")).explain(True)

input("\nAppuyez sur Entrée pour terminer (captures DAG sur http://localhost:4040)...")
spark.stop()
