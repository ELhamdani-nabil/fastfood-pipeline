import json
import time
from common import get_spark, BASE

spark = get_spark("p2_rdd")
sc = spark.sparkContext
sc.setLogLevel("WARN")
t0 = time.time()

# 1. Chargement + parsing
lines = sc.textFile(f"{BASE}/bronze/history/orders_history.jsonl")
rdd = lines.map(json.loads)

# 2. Uniquement les commandes livrées (transformation lazy)
delivered = rdd.filter(lambda o: o["status"] == "DELIVERED").cache()
print("Commandes DELIVERED :", delivered.count())          # action

# 3. Chiffre d'affaires par ville
ca = (delivered.map(lambda o: (o["city"], o["amount_mad"]))
      .reduceByKey(lambda a, b: a + b))
print("\n=== CA par ville (MAD) ===")
for city, v in sorted(ca.collect()):
    print(f"{city:12s} {v:12.2f}")

# 4. Panier moyen par ville (somme et compte en un seul passage)
avg = (delivered.map(lambda o: (o["city"], (o["amount_mad"], 1)))
       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
       .mapValues(lambda s: (s[0] / s[1], s[1])))
print("\n=== Panier moyen par ville ===")
for city, (m, n) in sorted(avg.collect()):
    print(f"{city:12s} {m:8.2f} MAD  ({n} commandes)")

# 5. Top 5 restaurants par nombre de commandes livrées
top5 = (delivered.map(lambda o: (o["restaurant_id"], 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda x: -x[1]))
print("\n=== Top 5 restaurants ===")
for r, n in top5:
    print(r, n)

print("\nTemps RDD (s) :", round(time.time() - t0, 2))

# 6. Lineage
print("\n=== Lineage (toDebugString) ===")
print(ca.toDebugString().decode())

input("\nAppuyez sur Entrée pour terminer (capture du DAG sur http://localhost:4040)...")
spark.stop()
