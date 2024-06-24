# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# I added these libraries to the workspace library. Otherwise fro each session, you have to run this line. 

%pip install openai num2words matplotlib plotly scipy scikit-learn pandas tiktoken pydocumentdb

# MARKDOWN ********************

# #### Read the data from Movies table and add to the dataframe

# CELL ********************

df = spark.sql("SELECT * FROM <lakehousename>.<movies table name>")
df = df.toPandas()
display(df)

# MARKDOWN ********************

# #### Clean the data

# CELL ********************

#data preparation

import re
import pandas as pd

def normalize_text(s, sep_token = " \n "):
    s = re.sub(r'\s+',  ' ', s).strip()
    s = re.sub(r". ,","",s)
    s = s.replace("..",".")
    s = s.replace(". .",".")
    s = s.replace("\n", "")
    s = s.strip()
    
    return s

df['synopsis'] = df['synopsis'].astype(str)
pd.options.mode.chained_assignment = None
df['synopsis']= df["synopsis"].apply(lambda x : normalize_text(x))
df['text'] = df[['title', 'genre', 'synopsis']].astype(str).agg(' '.join, axis=1)

print(df.head())

# MARKDOWN ********************

# #### Open AI key and Endpoint

# CELL ********************

import openai
from openai.embeddings_utils import get_embedding, cosine_similarity
import re
from pyspark.sql.functions import col, udf, concat, col, lit

API_KEY = ""
RESOURCE_ENDPOINT = "https://azure-openai-dnai.openai.azure.com/"

openai.api_type = "azure"
openai.api_key = API_KEY
openai.api_base = RESOURCE_ENDPOINT
openai.api_version = "2022-12-01"

df['ada_v2'] = df["text"].apply(lambda x : get_embedding(x, engine = 'embedding-ada')) 

print(df.head())


# MARKDOWN ********************

# #### Movie Search

# CELL ********************

def search_docs(df, user_query, top_n=3, to_print=True):
    embedding = get_embedding(
        user_query,
        engine="embedding-ada" 
    )
    df["similarities"] = df.ada_v2.apply(lambda x: cosine_similarity(x, embedding))

    res = (
        df.sort_values("similarities", ascending=False)
        .head(top_n)
    )
    if to_print:
        display(res)
    return res


res = search_docs(df, "alien movies", top_n=10)
