import os
from dotenv import load_dotenv
from prefect.blocks.system import Secret
from prefect.infrastructure import DockerContainer
from prefect.filesystems import GitHub

load_dotenv()

FILESTORER_ARCHIVE_PASSWORD = os.getenv('FILESTORER_ARCHIVE_PASSWORD')
if FILESTORER_ARCHIVE_PASSWORD:
    secret_block = Secret(value=FILESTORER_ARCHIVE_PASSWORD)
    secret_block.save('filestorer-archive-password', overwrite=True)
else:
    print("FILESTORER_ARCHIVE_PASSWORD is empty!")

FILESTORER_AUTH_TOKEN = os.getenv('FILESTORER_AUTH_TOKEN')
if FILESTORER_AUTH_TOKEN:
    secret_block = Secret(value=FILESTORER_AUTH_TOKEN)
    secret_block.save('filestorer-auth-token', overwrite=True)
else:
    print("FILESTORER_AUTH_TOKEN is empty!")

DATABASE_URL = os.getenv('DATABASE_URL')
if DATABASE_URL:
    secret_block = Secret(value=DATABASE_URL)
    secret_block.save('database-url', overwrite=True)
else:
    print("DATABASE_URL is empty!")

docker_block = DockerContainer(
    image="fserg/prefect-sales-recsys:latest", auto_remove=True)
docker_block_uuid = docker_block.save("sales-recsys-docker", overwrite=True)
print(f"Docker container: {docker_block_uuid}")


github_block = GitHub(
    repository="https://github.com/FSerg/sales-recsys.git",
    # access_token=<my_access_token> # only required for private repos
)
# github_block.get_directory("folder-in-repo") # specify a subfolder of repo
github_block_uuid = github_block.save("sales-recsys-repo", overwrite=True)
print(f"Github block: {github_block_uuid}")
