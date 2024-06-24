# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f35b0518-b19b-470b-92c5-685d460aa13a",
# META       "default_lakehouse_name": "Leme_LH_AI",
# META       "default_lakehouse_workspace_id": "a854d162-6bf9-4073-917e-14a11ce272aa",
# META       "known_lakehouses": [
# META         {
# META           "id": "f35b0518-b19b-470b-92c5-685d460aa13a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Install Libraries

# CELL ********************

%pip install openai --upgrade num2words matplotlib plotly scipy scikit-learn pandas tiktoken
#%pip install "openai==0.28.1" 
##https://learn.microsoft.com/en-us/azure/ai-services/openai/tutorials/embeddings?tabs=python%2Ccommand-line&pivots=programming-language-python

# MARKDOWN ********************

# ### OpenAI Variables

# CELL ********************

import openai
import requests

openai.api_key = "eb32babd4bd84400b401c57630a3fc5f"
openai.api_version ="2023-06-01-preview"
aoai_endpoint = "https://openaitesteise.openai.azure.com/"
openai.api_type = "azure"  

##Dataset: https://data.london.gov.uk/census/lwzc/

#credential = AzureKeyCredential(str(openai.api_key)) ##message informing that needs to be a string
##https://cookbook.openai.com/examples/get_embeddings_from_dataset
##https://platform.openai.com/docs/guides/embeddings/use-cases
##https://learn.microsoft.com/en-us/azure/ai-services/openai/tutorials/embeddings?tabs=python%2Ccommand-line&pivots=programming-language-python
##https://radix.ai/blog/2021/3/a-guide-to-building-document-embeddings-part-1/
##https://platform.openai.com/docs/guides/embeddings/use-cases


# MARKDOWN ********************

# ## Break of compatbility of open AI after version 28.

# MARKDOWN ********************

# ### Functions

# CELL ********************

import re
def normalize_text(s, sep_token = " \n "):
    s = re.sub(r'\s+',  ' ', s).strip()
    s = re.sub(r". ,","",s)
    # remove all instances of multiple spaces
    s = s.replace("..",".")
    s = s.replace(". .",".")
    s = s.replace("\n", "")
    s = s.strip()
    return s

# CELL ********************

import numpy as np
import pandas as pd
from ast import literal_eval


def get_embedding(text, model="text-embedding-ada-002"):
   text = text.replace("\n", " ")
   return client.embeddings.create(input = [text], model=model).data[0].embedding

def cosine_similarity(a, b):
    # Convert the input arrays to numpy arrays
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)

    # Check for empty arrays or arrays with zero norms
    if np.all(a == 0) or np.all(b == 0):
        return 0.0

    dot_product = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    
    similarity = dot_product / (norm_a * norm_b)
    return similarity

def convert_to_array(embedding_str):
    try:
        return literal_eval(embedding_str)
    except (ValueError, SyntaxError):
        return np.nan


def search_docs(df, user_query, top_n=4, to_print=True):
    embedding = get_embedding(user_query, model="text-embedding-ada-002")

    df['ada_embedding'] = df['ada_embedding'].apply(literal_eval)
    df['ada_embedding'] = df['ada_embedding'].apply(convert_to_array)
    df = df.dropna(subset=['ada_embedding'])    

    df = df[df['ada_embedding'].apply(lambda x: isinstance(x, np.ndarray))]

    # Calculate cosine similarity for each row in the DataFrame
    df["similarities"] = df.ada_embedding.apply(lambda x: cosine_similarity(x, embedding))

    # Sort and get the top N results
    res = df.sort_values("similarities", ascending=False).head(top_n)

    if to_print:
        display(res)
    return res



# MARKDOWN ********************

# ### Creating the embeddings

# CELL ********************

import pandas as pd
import openai
from openai import AzureOpenAI

#openai.api_key = "eb32babd4bd84400b401c57630a3fc5f"
#openai.api_key = "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"
#api_version= "2023-06-01-preview"
#aoai_endpoint = "https://openaitesteise.openai.azure.com/"

client = AzureOpenAI(
  api_key = openai.api_key,  
  api_version = "2023-05-15",
  azure_endpoint = aoai_endpoint
)

input_datapath = "/lakehouse/default/Files/Files_CSV/LWZC Classification.csv"  # to save space, we provide a pre-filtered dataset
df = pd.read_csv(input_datapath, index_col=0, nrows=20)  # Read only the first 20 rows




text_columns = ['LA name', 'Group description', 'Subgroup description']
df['combined'] = df[text_columns].astype(str).apply(lambda row: ' '.join(row), axis=1)

df['combined'] = df["combined"].apply(lambda x : normalize_text(x))

def generate_embeddings(text, model="text-embedding-ada-002"): # model = "deployment_name"
    return client.embeddings.create(input = [text], model=model).data[0].embedding

df['ada_embedding'] = df["combined"].apply(lambda x: get_embedding(x, model='text-embedding-ada-002'))
df.to_csv('/lakehouse/default/Files/Files_CSV/output/LWZC Classification_embeddindgs_20rows_v1.csv', index=False)

#df['ada_embedding'] = df["combined"].apply(lambda x: get_embedding(x, model='text-embedding-ada-002'))
#df.to_csv('output/LWZC Classification_embeddindgs.csv', index=False)


# CELL ********************


import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/Files_CSV/output/LWZC Classification_embeddindgs.csv"
df = pd.read_csv("/lakehouse/default/" + "Files/Files_CSV/output/LWZC Classification_embeddindgs_20rows_v1.csv")
df['ada_embedding'] = df.ada_embedding.apply(eval).apply(np.array)
df.head(20)


# MARKDOWN ********************

# ### Search

# CELL ********************



# Assuming df is your DataFrame with 'ada_embedding' column
embedding = get_embedding("Professional", model="text-embedding-ada-002")
df["similarities"] = df['ada_embedding'].apply(lambda x: cosine_similarity(x, embedding))
res = df.sort_values("similarities", ascending=False).head(4)
# Display the results
res.head()
# Display the results



# MARKDOWN ********************

# ## Search with  TF-IDF vectors

# CELL ********************

###Each column in tfidf_df corresponds to a unique term (word) in the combined text, and the values in each row represent the TF-IDF score of that term in the respective document (row). This can be considered a numerical representation or embedding of the text data based on its TF-IDF characteristics.
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

# Load the dataset
input_datapath = "/lakehouse/default/Files/Files_CSV/LWZC Classification.csv"
df = pd.read_csv(input_datapath, index_col=0)

# Extract text columns for TF-IDF
text_columns = ['LA name', 'Group description', 'Subgroup description']
text_data = df[text_columns].astype(str)

# Concatenate text columns to form sentences
sentences = text_data.apply(lambda row: ' '.join(row), axis=1)

# Create TF-IDF vectorizer
vectorizer = TfidfVectorizer()

# Fit and transform the sentences to get TF-IDF embeddings
tfidf_embeddings = vectorizer.fit_transform(sentences)

# Convert the TF-IDF embeddings to a DataFrame
tfidf_df = pd.DataFrame(tfidf_embeddings.toarray(), columns=vectorizer.get_feature_names_out(), index=df.index)

# Add TF-IDF embeddings to the original DataFrame
df = pd.concat([df, tfidf_df], axis=1)

# Display the DataFrame with TF-IDF embeddings
print(df.head())


# MARKDOWN ********************

# ### Search

# CELL ********************

from sklearn.metrics.pairwise import cosine_similarity

query_text = "test test"

# Transform the query text using the same TF-IDF vectorizer
query_vector = vectorizer.transform([query_text])

# Calculate cosine similarity between the query vector and all document vectors
cosine_similarities = cosine_similarity(query_vector, tfidf_embeddings)

# Get the index of the most similar document
most_similar_index = cosine_similarities.argmax()

# Retrieve the most similar document
most_similar_document = df.iloc[most_similar_index]

# Display the most similar document
print(most_similar_document)

