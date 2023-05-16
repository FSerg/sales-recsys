import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret
from prefect.infrastructure import DockerContainer
from prefect.filesystems import GitHub

from prefect.deployments import Deployment
from main import main


load_dotenv()

FILESTORER_ARCHIVE_PASSWORD = os.getenv("FILESTORER_ARCHIVE_PASSWORD")
if FILESTORER_ARCHIVE_PASSWORD:
    secret_block = Secret(value=FILESTORER_ARCHIVE_PASSWORD)
    secret_block.save("filestorer-archive-password", overwrite=True)
else:
    print("FILESTORER_ARCHIVE_PASSWORD is empty!")

FILESTORER_AUTH_TOKEN = os.getenv("FILESTORER_AUTH_TOKEN")
if FILESTORER_AUTH_TOKEN:
    secret_block = Secret(value=FILESTORER_AUTH_TOKEN)
    secret_block.save("filestorer-auth-token", overwrite=True)
else:
    print("FILESTORER_AUTH_TOKEN is empty!")

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    secret_block = Secret(value=DATABASE_URL)
    secret_block.save("database-url", overwrite=True)
else:
    print("DATABASE_URL is empty!")

# Infrastructure Block
docker_block = DockerContainer(
    image="fserg/prefect-sales-recsys:latest", auto_remove=True)
docker_block_uuid = docker_block.save("sales-recsys-docker", overwrite=True)
print(f"Docker container: {docker_block_uuid}")

# Storage Block
github_block = GitHub(
    repository="https://github.com/FSerg/sales-recsys.git",
    # access_token=os.getenv("GITHUB_TOKEN") # only required for private repos
)
github_block_uuid = github_block.save("sales-recsys-repo", overwrite=True)
print(f"Github block: {github_block_uuid}")

deployment = Deployment.build_from_flow(
    name="sales-recsys-docker-deployment",
    flow=main,
    work_queue_name="agent1-queue",
    tags=["sales-recsys"],
    parameters={"filestorer_url": "https://da-files.f-pix.ru",
                "filename_csv_arch": "files.zip",
                "filename_result_arch": "result.zip",
                "table_name": "sales_lite",
                "wholesale_support": 0.005, "retail_support": 0.0001},
    storage=GitHub.load("sales-recsys-repo"),
    infrastructure=DockerContainer.load("sales-recsys-docker"),

)
deployment.apply()
