import os
import pyzipper
import psycopg2
import requests
import tempfile
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

from process import main_process


@flow(name="01. Sales-recsys main ETL-flow")
def main(filestorer_url: str,
         filename_csv_arch: str,
         filename_result_arch: str,
         table_name: str,
         wholesale_support: float, retail_support: float) -> None:
    temp_dir, file_path = download(filestorer_url, filename_csv_arch)
    if file_path == None:
        raise ValueError("Failed to download the file.")

    extract_csv(temp_dir, file_path)

    try:
        with psycopg2.connect(Secret.load("database-url").get()) as conn:
            prepare_tables(conn, table_name)
            import_csv(conn, temp_dir, table_name)

    except psycopg2.Error as e:
        raise ValueError(f"Error connecting to PostgreSQL: {e}")

    main_process(filestorer_url, filename_result_arch,
                 table_name, wholesale_support, retail_support)


@task(name="Download csv-files archive from filestorer")
def download(filestorer_url: str, filename_csv_arch: str):
    logger = get_run_logger()

    full_url = filestorer_url + "/download/" + filename_csv_arch
    logger.info(f"Downloading file from: {full_url}")

    secret_block = Secret.load("filestorer-auth-token")
    headers = {'Authorization': secret_block.get()}
    response = requests.get(full_url, stream=True, headers=headers)
    if response.status_code == 200:
        # temp_dir = tempfile.TemporaryDirectory()
        temp_dir = tempfile.mkdtemp()
        file_name = "files.zip"
        destination = os.path.join(temp_dir, file_name)
        with open(destination, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        logger.info("File downloaded successfully.")
        return temp_dir, destination
    else:
        logger.error("Failed to download the file.")
        logger.error(
            f"Code: {response.status_code}, response: {response.text}")
        return None, None


@task(name="Extract CSV-files")
def extract_csv(temp_dir: str, zip_file_path: str) -> None:
    logger = get_run_logger()
    secret_block = Secret.load("filestorer-archive-password")
    logger.info(f"Extracting {zip_file_path}")
    fsize = round(os.stat(zip_file_path).st_size / 1024, 2)
    logger.info(f"File size: {fsize} Kb")
    with pyzipper.AESZipFile(zip_file_path) as zf:
        zf.setpassword(secret_block.get().encode())
        if len(zf.namelist()) == 0:
            raise ValueError("Downloaded file is empty")
        zf.extractall(temp_dir)
    logger.info("CSV-files extracted successfully.")


@task(name="Prepare tables in DB")
def prepare_tables(conn, table_name: str):
    logger = get_run_logger()
    sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        doc_date TIMESTAMP,
        doc_number VARCHAR(20),
        doc_type VARCHAR(30),
        doc_id VARCHAR(36),
        product_code VARCHAR(20),
        product_name VARCHAR(255)
        )"""

    with conn.cursor() as cursor:
        cursor.execute(sql)
    logger.info(f"Table {table_name} created")

    # Clear table before loading
    sql = f"TRUNCATE {table_name}"
    with conn.cursor() as cursor:
        cursor.execute(sql)
    logger.info(f"Table {table_name} truncated")


@task(name="Import csv files into DB")
def import_csv(conn, temp_dir: str, table_name: str) -> None:
    logger = get_run_logger()
    files = os.listdir(temp_dir)
    csv_files = sorted([file for file in files if file.endswith(".csv")])
    logger.info(csv_files)

    with conn.cursor() as cursor:
        for csv_file in csv_files:
            csv_file_path_local = os.path.join(temp_dir, csv_file)
            if os.stat(csv_file_path_local).st_size <= 3:
                continue
            with open(csv_file_path_local, 'r') as f:
                logger.info(f"Importing {csv_file_path_local}")
                print(f"Processing file: {f.name}")
                print(f"Table name: {table_name}")
                cursor.copy_expert(
                    f"COPY {table_name}(doc_date, doc_number, doc_type, doc_id, product_code, product_name) FROM STDIN WITH DELIMITER ';' QUOTE '`' CSV", f)

        cursor.connection.commit()

    logger.info("CSV-files loaded")


if __name__ == "__main__":
    main(filestorer_url="https://da-files.f-pix.ru",
         filename_csv_arch="files.zip",
         filename_result_arch="result.zip",
         table_name="sales_lite",
         wholesale_support=0.005,
         retail_support=0.0001)
