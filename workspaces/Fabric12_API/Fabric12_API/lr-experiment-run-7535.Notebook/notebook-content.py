# Fabric notebook source


# MARKDOWN ********************

# # Track Machine Learning experiments and models
# 
# A machine learning model is a file that has been trained to recognize certain types of patterns. You train a model over a set of data, providing it an algorithm that it can use to reason over and learn from those data. Once you have trained the model, you can use it to reason over data that it hasn't seen before, and make predictions about that data.
# 
# In this notebook, you will learn the basic steps to run an experiment, add a model version to track run metrics and parameters and register a model.


# CELL ********************

import mlflow

# Set given experiment as the active experiment. If an experiment with this name does not exist, a new experiment with this name is created.
mlflow.set_experiment("mlexplondonstats")


# CELL ********************

import mlflow.sklearn
import numpy as np
from sklearn.linear_model import LogisticRegression
from mlflow.models.signature import infer_signature

# Start your training job with `start_run()`
with mlflow.start_run() as run:

    lr = LogisticRegression()
    X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
    y = np.array([0, 0, 1, 1, 1, 0])
    lr.fit(X, y)
    score = lr.score(X, y)
    signature = infer_signature(X, y)

    # Activate the MLFlow logging API to log your training job metrics
    print("test log_metrics.")
    mlflow.log_metric("score", score)

    # Activate the MLFlow logging API to log your training job parameters
    print("test log_params.")
    mlflow.log_param("alpha", "alpha")
    print("All done")
