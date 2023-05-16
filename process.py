import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from mlxtend.frequent_patterns import fpmax
import pyzipper
import requests

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

CHUNK_SIZE = 10000


@flow(name="02. Sales-recsys main data processing")
def main_process(filestorer_url: str,
                 filename_result_arch: str,
                 table_name: str,
                 wholesale_support: float, retail_support: float) -> None:
    logger = get_run_logger()
    logger.info("Data processing is started")

    engine = create_engine(Secret.load("database-url").get())
    try:
        with engine.connect() as conn:
            file1 = process_data(conn, table_name, trans_type="Реализация",
                                 file_name="result_wholesale.csv", support=wholesale_support)
            file2 = process_data(conn, table_name, trans_type="ЧекККМ",
                                 file_name="result_retail.csv", support=retail_support)
    except SQLAlchemyError as err:
        raise ValueError(
            f"Error during connection to the database: {err.__cause__}")

    files_to_compress = [file1, file2]
    result_file = zip_results(filename_result_arch, files_to_compress)
    store_results(filestorer_url, result_file)


@flow(name="03. Subflow processing", flow_run_name="Doc type: {trans_type}")
def process_data(conn, table_name: str, trans_type: str, file_name: str, support: float) -> str:
    logger = get_run_logger()

    query = get_transactions_query(table_name, trans_type)
    df = pd.DataFrame()
    for chunk_df in pd.read_sql_query(text(query), conn, chunksize=CHUNK_SIZE):
        df = pd.concat([df, chunk_df], ignore_index=True)

    fin_df = calc_related_products(prepare_df(df), support)
    fin_df[["product1", "product2"]].to_csv(
        file_name, header=False, index=False)
    logger.info(f"{trans_type} is done")
    return file_name


def get_transactions_query(table_name: str, doc_type: str) -> str:
    query = f"""
        SELECT doc_id, product_code FROM {table_name}
        WHERE LOWER(product_name) NOT LIKE '%пакет майка%' AND 
            LOWER(product_name) NOT LIKE '%колеровка%'
            AND doc_type = '{doc_type}'"""
    return query


@task(name="Calculate related products")
def calc_related_products(df: pd.DataFrame, support: float) -> pd.DataFrame:
    logger = get_run_logger()

    result_df = fpmax(df, min_support=support,
                      use_colnames=True, max_len=2, verbose=0)
    result_df['length'] = result_df['itemsets'].apply(lambda x: len(x))
    result_df = result_df[result_df['length'] >= 2]
    result_df['product1'] = result_df['itemsets'].apply(
        lambda x: list(x)[0] if x else None)
    result_df['product2'] = result_df['itemsets'].apply(
        lambda x: list(x)[1] if x else None)
    logger.info(f"Related products DF size: {result_df.shape}")
    return result_df


@task(name="Preparing dataframe (pivoting)")
def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()

    df['value'] = True
    pivot_df = df.groupby(['doc_id', 'product_code'])[
        'value'].first().unstack(fill_value=False)
    # rename columns to use the unique product codes
    pivot_df.columns = pivot_df.columns.astype(str)
    logger.info(f"Initial DF size: {df.shape}")
    return pivot_df


@task(name="Archiving results")
def zip_results(filename_result_arch: str, files_to_compress: str) -> str:
    logger = get_run_logger()

    zip_file_name = os.path.join(os.getcwd(), filename_result_arch)
    secret_block = Secret.load("filestorer-archive-password")
    # with pyzipper.AESZipFile(zip_file_name, 'w', compression=pyzipper.ZIP_STORED, encryption=pyzipper.WZ_AES) as zipf:
    with pyzipper.AESZipFile(zip_file_name, 'w', compression=pyzipper.ZIP_BZIP2, encryption=pyzipper.WZ_AES) as zipf:
        zipf.setpassword(secret_block.get().encode())
        for file in files_to_compress:
            zipf.write(file)

    fsize = round(os.stat(zip_file_name).st_size / 1024, 2)
    logger.info(f"File {zip_file_name} is ready, size: {fsize} Kb")
    return zip_file_name


@task(name="Send results to filestorer")
def store_results(filestorer_url: str, zip_file_name: str) -> None:
    logger = get_run_logger()

    full_url = filestorer_url + "/upload"
    logger.info(f"Uploading file to: {full_url}")
    secret_block = Secret.load("filestorer-auth-token")
    headers = {'Authorization': secret_block.get()}

    files = {'file': open(zip_file_name, 'rb')}
    response = requests.post(full_url, files=files, headers=headers)
    if response.status_code == 200:
        logger.info("File uploaded successfully.")
    else:
        logger.error(f"Failed to upload file: {zip_file_name}")
        logger.error(
            f"Code: {response.status_code}, response: {response.text}")
