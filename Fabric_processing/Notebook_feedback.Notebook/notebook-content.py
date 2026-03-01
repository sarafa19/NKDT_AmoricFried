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
# META     },
# META     "environment": {
# META       "environmentId": "b32b72c1-c02d-b776-4e2d-d5874a4ecf75",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import requests
import os
import json
from openai import AzureOpenAI
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import col
import hashlib
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

api_key = mssparkutils.credentials.getSecret(
    "https://amoricfriedkeys.vault.azure.net/",   
    "AZURE-OPENAI-API-KEY"  
)

endpoint = mssparkutils.credentials.getSecret(
    "https://amoricfriedkeys.vault.azure.net/",
    "AZURE-OPENAI-ENDPOINT"
)

feedback_url = mssparkutils.credentials.getSecret(
    "https://amoricfriedkeys.vault.azure.net/",
    "FEEDBACK-URL"
)

print("Secrets loaded:", bool(api_key), bool(endpoint), bool(feedback_url))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 1. Configuration Azure OpenAI

# CELL ********************

client = AzureOpenAI(
    api_key=api_key,
    api_version="2024-12-01-preview",
    azure_endpoint=endpoint,
)

MODEL = "gpt-4.1"

if not api_key or not endpoint:
    raise RuntimeError("Azure OpenAI configuration is incomplete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 2. Récupération des feedbacks

# CELL ********************

print("Récupération des feedbacks...")
try:
    resp = requests.get(feedback_url, timeout=10)
    resp.raise_for_status()
    feedback_list = resp.json()
except Exception as e:
    raise RuntimeError(f"Erreur lors de l'appel à l'API feedback : {e}")

print(f"➡️ {len(feedback_list)} feedbacks reçus")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def make_feedback_id(username, campaign_id, comment):
    raw = f"{username}|{campaign_id}|{comment}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for fb in feedback_list:
    fb["feedback_id"] = make_feedback_id(
        fb.get("username", ""),
        fb.get("campaign_id", ""),
        fb.get("comment", "")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Charger les IDs déjà présents dans Bronze (si la table existe)
try:
    df_bronze_existing = spark.table("bronze_feedback")
    existing_ids = {row.feedback_id for row in df_bronze_existing.select("feedback_id").collect()}
except:
    existing_ids = set()  # première exécution : table vide

# 4. Filtrer uniquement les nouveaux feedbacks
new_feedbacks = [fb for fb in feedback_list if fb["feedback_id"] not in existing_ids]

# 5. Si aucun nouveau feedback, on arrête proprement
if not new_feedbacks:
    print("Aucun nouveau feedback à insérer dans Bronze.")
else:
    # 6. Création du DataFrame Bronze
    df_bronze = spark.createDataFrame(new_feedbacks)
    df_bronze = df_bronze.withColumn("ingested_at", F.current_timestamp())

    # 7. Écriture en mode append (uniquement les nouveaux)
    df_bronze.write.format("delta").mode("append").saveAsTable("bronze_feedback")

    df_bronze.show(10)
    print(f"{len(new_feedbacks)} nouveaux feedbacks insérés dans Bronze.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Fonctions de quality check

# CELL ********************

def normalize_date(date_str):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def normalize_campaign_id(camp_id):
    if not camp_id:
        return None
    camp_id = str(camp_id).upper().replace("CAMP", "")
    if not camp_id.isdigit():
        return None
    return f"CAMP{int(camp_id):03d}"

def normalize_comment(comment):
    if not comment or not comment.strip():
        return None
    return comment.strip()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 5. Traitement feedbacks
# -----------------------------
valid_feedbacks = []
for fb in new_feedbacks:
    fb["feedback_date"] = normalize_date(fb.get("feedback_date", ""))
    fb["campaign_id"] = normalize_campaign_id(fb.get("campaign_id", ""))
    fb["comment"] = normalize_comment(fb.get("comment", ""))

    if None in (fb["feedback_date"], fb["campaign_id"], fb["comment"]):
        print(f" Feedback ignoré (mauvais format) : {fb}")
        continue

    valid_feedbacks.append(fb)

print(f"✅ {len(valid_feedbacks)} feedbacks valides après normalisation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------
# 6. Analyse IA + écriture Delta
# -----------------------------
rows_to_write = []

for fb in new_feedbacks:
    username = fb["username"]
    feedback_date = fb["feedback_date"]
    campaign_id = fb["campaign_id"]
    comment = fb["comment"]

    prompt = f"""
You are an AI specialized in multilingual marketing feedback analysis.*

Your task:
1. Detect the language of the comment.
2. Translate the comment to English and include it in the output.
3. Perform sentiment analysis.
4. Classify the marketing dimension.
5. Extract key adjectives (in English only).
6. Output a STRICT JSON object following EXACTLY this schema:

{{
  "sentiment_label": "Positive | Negative | Neutral",
  "star_rating": 0-5,
  "marketing_dimension": "Creativity | Clarity | Engagement | Global Strategy",
  "key_adjectives": one single word,
  "english_translation": "The comment translated to English"
}}

Rules:
- ALWAYS output valid JSON.
- NEVER add extra fields except "english_translation".
- NEVER change field names.
- ALWAYS output adjectives in English.
- ALWAYS choose one of the allowed marketing dimensions.
- If the comment is unclear, choose the closest category.

Comment to analyze:
\"\"\"{comment}\"\"\"
"""

    print(f"\n🧠 Analyse du feedback : {comment}")
    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    analysis = json.loads(response.choices[0].message.content)

    print("Analyse IA :")
    print(json.dumps(analysis, indent=4))

    # Construction de la ligne pour Delta
    rows_to_write.append({
        "feedback_id": fb["feedback_id"],
        "username": username,
        "feedback_date": feedback_date,
        "campaign_id": campaign_id,
        "comment": comment,
        "english_translation": analysis["english_translation"], 
        "marketing_dimension": analysis["marketing_dimension"],
        "star_rating": analysis["star_rating"],
        "sentiment_label": analysis["sentiment_label"],
        "key_adjectives": analysis["key_adjectives"],
        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
from delta.tables import DeltaTable

# -----------------------------
# 1️⃣ Transformer la liste de dictionnaires en DataFrame Spark
# -----------------------------
df_silver_new = spark.createDataFrame(rows_to_write)

# -----------------------------
# 2️⃣ Définir les règles de validation
# -----------------------------
valid_sentiments = ["Positive", "Negative", "Neutral"]
valid_star_ratings = list(range(0, 6))  # 0 à 5 inclus
valid_marketing_dimensions = ["Creativity", "Clarity", "Engagement", "Global Strategy"]

# -----------------------------
# 3️⃣ Filtrer uniquement les lignes valides
# -----------------------------
df_valid = df_silver_new.filter(
    (col("sentiment_label").isin(valid_sentiments)) &
    (col("star_rating").isin(valid_star_ratings)) &
    (col("marketing_dimension").isin(valid_marketing_dimensions))
)

print(f"Lignes valides à insérer/mettre à jour : {df_valid.count()} / {df_silver_new.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_silver = DeltaTable.forName(spark, "silver_feedback")

(
    delta_silver.alias("t")
    .merge(
        df_valid.alias("s"),
        "t.feedback_id = s.feedback_id"
    )
    .whenMatchedUpdateAll()    
    .whenNotMatchedInsertAll() 
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
