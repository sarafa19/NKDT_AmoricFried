# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7479bac9-a6e2-4d25-9748-4393cb10fc35",
# META       "default_lakehouse_name": "Sales_Lakehouse",
# META       "default_lakehouse_workspace_id": "3b672d63-a2ee-4726-92cb-a96df175d490",
# META       "known_lakehouses": [
# META         {
# META           "id": "7479bac9-a6e2-4d25-9748-4393cb10fc35"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, trim, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from notebookutils import mssparkutils
from pyspark.sql import Row

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Création de la table de Tracking**

# CELL ********************

schema = StructType([
    StructField("file_name", StringType(), False),
    StructField("file_path", StringType(), False),
    StructField("processed_at", TimestampType(), False)
])

df_empty = spark.createDataFrame([], schema)

if not spark.catalog.tableExists("file_tracking"):
    df_empty.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("file_tracking")
        
    print("Table 'file_tracking' créée avec succès.")
else:
    print("La table 'file_tracking' existe déjà.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import current_timestamp
from pyspark.sql import Row

folder_path = "Files/Bronze"

# 1️⃣ Lister les fichiers du dossier
files = mssparkutils.fs.ls(folder_path)
csv_files = [f for f in files if f.path.endswith(".csv")]

if not csv_files:
    raise Exception("Aucun fichier CSV trouvé.")

# 2️⃣ Charger la table de tracking (si elle existe)
if spark.catalog.tableExists("file_tracking"):
    df_tracking = spark.table("file_tracking")
    processed_files = [row.file_path for row in df_tracking.select("file_path").collect()]
else:
    processed_files = []

# 3️⃣ Garder uniquement les fichiers NON traités
new_files = [f for f in csv_files if f.path not in processed_files]

if not new_files:
    print("Aucun nouveau fichier à traiter.")
else:
    print(f"{len(new_files)} nouveau(x) fichier(s) trouvé(s).")

    # 4️⃣ Charger tous les nouveaux fichiers
    paths = [f.path for f in new_files]

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(paths)
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = (
    df
        # Trim des colonnes texte
        .select([trim(col(c)).alias(c) for c in df.columns])

        # Conversion des types
        .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("unit_price", col("unit_price").cast("double"))

        .dropDuplicates()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean.write.mode("append").format("delta").saveAsTable("silver_sales")
taille=df_clean.count()
print(f"Nombre de lignes enregistrées:{taille}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5️⃣ Mettre à jour la table de tracking
tracking_rows = [
    Row(file_name=f.name, file_path=f.path)
    for f in new_files
]

df_new_tracking = spark.createDataFrame(tracking_rows) \
                        .withColumn("processed_at", current_timestamp())

df_new_tracking.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("file_tracking")

print("Table de tracking mise à jour.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
