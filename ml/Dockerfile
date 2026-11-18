# Jupyter environment for the fraud model (runs in Docker, not in the Windows venv).
# Base image already ships pandas, scikit-learn, matplotlib and nbconvert.
FROM jupyter/scipy-notebook:2023-10-20

RUN pip install --no-cache-dir \
    "xgboost==2.0.3" \
    "snowflake-connector-python[pandas]==3.12.3"
