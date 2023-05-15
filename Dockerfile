FROM prefecthq/prefect:2.10.6-python3.11
COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
