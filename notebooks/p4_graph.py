from pyspark.sql import functions as F
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from common import get_spark, BASE

spark = get_spark("p4_graph")
sc = spark.sparkContext
sc.setLogLevel("WARN")
sc.setCheckpointDir(f"{BASE}/checkpoints/graph")

silver = spark.read.parquet(f"{BASE}/silver/orders_history/")   # DELIVERED, dédoublonné
print("Lignes Silver :", silver.count())

# 1. Sommets et arêtes
clients = (silver.select(F.col("customer_id").alias("id")).distinct()
           .withColumn("type", F.lit("client"))
           .withColumn("city", F.lit(None).cast("string"))
           .withColumn("category", F.lit(None).cast("string")))
restos = (silver.select(F.col("restaurant_id").alias("id"), "city", "category").distinct()
          .withColumn("type", F.lit("restaurant")))
vertices = clients.select("id", "type", "city", "category") \
    .unionByName(restos.select("id", "type", "city", "category"))

edges = (silver.groupBy(F.col("customer_id").alias("src"), F.col("restaurant_id").alias("dst"))
         .agg(F.count("*").alias("nb_orders"), F.sum("amount_mad").alias("total_amount")))

g = GraphFrame(vertices, edges)
g.vertices.cache(); g.edges.cache()
print("Sommets :", g.vertices.count(), "| Arêtes :", g.edges.count())

# 2. Degrés
print("\n=== Top 10 restaurants par nombre de clients distincts (inDegree) ===")
(g.inDegrees.join(vertices.filter("type = 'restaurant'"), "id")
 .orderBy(F.desc("inDegree")).select("id", "city", "category", "inDegree").show(10))
print("=== Top 10 clients par nombre de restaurants différents (outDegree) ===")
g.outDegrees.orderBy(F.desc("outDegree")).show(10)

# 3. PageRank
print("=== Top 10 restaurants par PageRank ===")
pr = g.pageRank(resetProbability=0.15, maxIter=10)
(pr.vertices.filter("type = 'restaurant'").orderBy(F.desc("pagerank"))
 .select("id", "city", "category", F.round("pagerank", 4).alias("pagerank")).show(10))
print("=== Rappel : top 5 par nombre de commandes (Partie 2, sur Silver dédoublonné) ===")
silver.groupBy("restaurant_id").count().orderBy(F.desc("count")).show(5)

# 4. Composantes connexes
cc = g.connectedComponents()
sizes = cc.groupBy("component").count()
print("Nombre de composantes connexes :", sizes.count())
print("Taille de la plus grande       :", sizes.agg(F.max("count")).first()[0])

# 5. Motifs : un client qui a commandé dans 2 restaurants différents de même catégorie
motifs = (g.find("(c)-[e1]->(r1); (c)-[e2]->(r2)")
          .filter("c.type = 'client' AND r1.category = r2.category AND r1.id < r2.id"))
print("\n=== Exemples de motifs ===")
motifs.select(F.col("c.id").alias("client"), F.col("r1.id").alias("resto_1"),
              F.col("r2.id").alias("resto_2"), F.col("r1.category").alias("categorie")).show(10)
print("Nombre de motifs :", motifs.count())

# Recommandation simple pour un client : restaurants de même catégorie que ceux
# qu'il fréquente, choisis par des clients qui ont des restaurants en commun avec lui.
client, cat = motifs.select(F.col("c.id"), F.col("r1.category")).first()
print(f"Recommandations de la catégorie '{cat}' pour le client {client}")
visited = edges.filter(F.col("src") == client).select(F.col("dst").alias("id"))
cats = spark.createDataFrame([(cat,)], ["category"])
peers = (edges.join(visited, edges.dst == visited.id).filter(F.col("src") != client)
         .select(F.col("src").alias("peer")).distinct())
reco = (edges.join(peers, edges.src == peers.peer)
        .select(F.col("dst").alias("id"))
        .join(visited, "id", "left_anti")
        .join(vertices, "id").join(cats, "category")
        .groupBy("id", "city", "category").count().orderBy(F.desc("count")))
reco.show(5)

# 6. aggregateMessages : CA total reçu par restaurant
print("=== CA par restaurant via aggregateMessages ===")
agg = g.aggregateMessages(F.round(F.sum(AM.msg), 2).alias("ca"), sendToDst=AM.edge["total_amount"])
agg.orderBy(F.desc("ca")).show(10)
print("=== Vérification par groupBy ===")
(edges.groupBy("dst").agg(F.round(F.sum("total_amount"), 2).alias("ca"))
 .orderBy(F.desc("ca")).show(10))

input("\nAppuyez sur Entrée pour terminer (captures PageRank/composantes + http://localhost:4040)...")
spark.stop()
