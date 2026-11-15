from common import get_spark

spark = get_spark("p1_cluster", packages=False)
sc = spark.sparkContext

# Un petit job force les executors à s'enregistrer auprès du Master
print("Test distribué     :", sc.parallelize(range(100), 4).sum())

print("Spark version      :", spark.version)
print("Master             :", sc.master)
print("appName            :", sc.appName)
print("defaultParallelism :", sc.defaultParallelism)

input("Appuyez sur Entrée pour terminer (capture de http://localhost:8080 maintenant)...")
spark.stop()
