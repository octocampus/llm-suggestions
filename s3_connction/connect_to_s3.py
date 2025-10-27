import io
import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

REGION_NAME = os.getenv("region_name")
AWS_ACCESS_KEY_ID = os.getenv("aws_access_key_id")
AWS_SECRET_ACCESS_KEY = os.getenv("aws_secret_access_key")
ENDPOINT_URL = os.getenv("endpoint_url")

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    config=Config(signature_version="s3v4"),
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def list_objects_with_prefix(s3_client, bucket_name, prefix=""):
    """Return a list of object keys under a prefix (handles pagination)."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                keys.append(obj["Key"])
    return keys


def get_all_created_models(s3_client, bucket_name):
    """List all objects in the models directory (returns list)."""
    return list_objects_with_prefix(s3_client, bucket_name, prefix="models/")


def get_all_created_dags(s3_client, bucket_name):
    """List all objects in the dags directory (returns list)."""
    return list_objects_with_prefix(s3_client, bucket_name, prefix="dags/")


def get_all_created(s3_client, bucket_name):
    """List all objects in the bucket (returns list)."""
    return list_objects_with_prefix(s3_client, bucket_name, prefix="")


def check_model_yaml_file_existance(s3_client, bucket_name, s3_file_name):
    """Check if a file exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_file_name)
        return True
    except Exception:
        return False


def check_model_yaml_file_existance_for_update_delete(
    s3_client, bucket_name, s3_file_name
):
    """Check if file exists, print error if it doesn't."""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_file_name)
        return True
    except Exception:
        print(f"Error: File {s3_file_name} doesn't exist")
        return False


def read_file(s3_client, bucket_name, s3_file_name):
    """Read and return the content of a file from S3."""
    if check_model_yaml_file_existance(s3_client, bucket_name, s3_file_name):
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
            file_content = response["Body"].read().decode("utf-8")
            return file_content
        except Exception as e:
            print(f"Error reading {s3_file_name} from S3: {e}")
            return None
    else:
        print(f"File {s3_file_name} does not exist in bucket {bucket_name}")
        return None



def list_folders(s3_client, bucket_name, prefix=""):
    """List all folders (prefixes) in a given path (non-recursive)."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        )

        folders = []
        if "CommonPrefixes" in response:
            print(f"\nFolders in '{prefix or 'root'}':")
            for prefix_info in response["CommonPrefixes"]:
                folder = prefix_info["Prefix"]
                folders.append(folder)
                print(f"  📁 {folder}")
        else:
            print(f"No folders found in '{prefix or 'root'}'")

        # Also show files in current level
        if "Contents" in response:
            print(f"\nFiles in '{prefix or 'root'}':")
            for obj in response["Contents"]:
                if obj["Key"] != prefix:
                    print(f"  📄 {obj['Key']}")

        return folders
    except Exception as e:
        print(f"Error listing folders: {e}")
        return []


def explore_bucket(s3_client, bucket_name, prefix="", max_depth=3, current_depth=0):
    """Recursively explore bucket structure and display as tree (prints)."""
    if current_depth >= max_depth:
        return

    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        )

        indent = "  " * current_depth

        # Display folders
        if "CommonPrefixes" in response:
            for prefix_info in response["CommonPrefixes"]:
                folder = prefix_info["Prefix"]
                folder_name = folder.rstrip("/").split("/")[-1]
                print(f"{indent}📁 {folder_name}/")
                # Recursively explore subfolders
                explore_bucket(
                    s3_client, bucket_name, folder, max_depth, current_depth + 1
                )

        # Display files at current level
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"] != prefix and "/" not in obj["Key"][len(prefix) :]:
                    file_name = obj["Key"].split("/")[-1]
                    size = obj["Size"]
                    print(f"{indent}📄 {file_name} ({size} bytes)")

    except Exception as e:
        print(f"Error exploring bucket: {e}")





import errno

def download_file_from_s3(s3_client, bucket_name, s3_key, local_path):
    """
    Download a single file from S3 and save it to local_path.
    Streams content to avoid loading entire object into memory.
    """
    try:
        # Normalize local_path and ensure directory exists
        local_path = os.path.normpath(local_path)
        local_dir = os.path.dirname(local_path) or "."
        os.makedirs(local_dir, exist_ok=True)

        with s3_client.get_object(Bucket=bucket_name, Key=s3_key)["Body"] as stream:
            with open(local_path, "wb") as f:
                for chunk in iter(lambda: stream.read(4096), b""):
                    if not chunk:
                        break
                    f.write(chunk)

        print(f"Downloaded s3://{bucket_name}/{s3_key} -> {local_path}")
        return True
    except OSError as ose:
        # More helpful error messages for common filesystem problems
        if ose.errno == errno.EACCES:
            print(f"Permission denied when writing to {local_path}: {ose}")
        elif ose.errno == errno.EROFS:
            print(f"Read-only file system when writing to {local_path}: {ose}")
        else:
            print(f"OS error downloading s3://{bucket_name}/{s3_key}: {ose}")
        return False
    except Exception as e:
        print(f"Error downloading s3://{bucket_name}/{s3_key}: {e}")
        return False


def download_prefix_from_s3(s3_client, bucket_name, prefix, local_dir, strip_prefix=True):
    """
    Download all objects under `prefix` to `local_dir`.
    If strip_prefix True, the prefix will be removed from the local path so
    dags/foo.py -> <local_dir>/foo.py instead of <local_dir>/dags/foo.py.
    Returns list of downloaded local paths.
    """
    downloaded = []

    # Normalize local_dir and ensure it's absolute and writable
    local_dir = os.path.abspath(os.path.expanduser(local_dir))
    try:
        os.makedirs(local_dir, exist_ok=True)
    except OSError as e:
        print(f"Unable to create local_dir '{local_dir}': {e}")
        return downloaded

    # Quick writable check
    if not os.access(local_dir, os.W_OK):
        print(f"Local dir '{local_dir}' is not writable. Choose another directory (e.g. ~/Downloads or /tmp).")
        return downloaded

    keys = list_objects_with_prefix(s3_client, bucket_name, prefix=prefix)

    for key in keys:
        # Normalize key so it won't start with a leading slash
        norm_key = key.lstrip("/")

        # Skip "directory placeholder" keys (if any)
        if norm_key.endswith("/") and norm_key == prefix.lstrip("/"):
            continue

        # Compute relative path
        if strip_prefix and norm_key.startswith(prefix.lstrip("/")):
            relative_path = norm_key[len(prefix.lstrip("/")) :]
        else:
            relative_path = norm_key

        # If relative_path is empty or just '/', skip
        if not relative_path or relative_path == "/":
            continue

        # Ensure we don't accidentally write to filesystem root if relative_path starts with '/'
        relative_path = relative_path.lstrip("/")

        # Compose local path safely
        local_path = os.path.normpath(os.path.join(local_dir, relative_path))

        # Security check: make sure local_path is inside local_dir (avoid path traversal)
        if not os.path.commonpath([local_dir, local_path]) == local_dir:
            print(f"Skipping suspicious path: s3 key {key} -> local path {local_path}")
            continue

        success = download_file_from_s3(s3_client, bucket_name, key, local_path)
        if success:
            downloaded.append(local_path)

    return downloaded




bucket_name = "qupid"


if __name__ == "__main__":
    # Example: list dags keys (prints result)
    # dags = get_all_created_models(s3_client, bucket_name)
    # print("DAG objects found:")
    # for k in dags:
    #     print(" ", k)

    # Example: Download a single file from dags (if you know the key)
    # download_file_from_s3(s3_client, bucket_name, "dags/example_dag.py", "./downloads/example_dag.py")

    # Example: Download all files under the dags/ prefix to local ./downloads/dags/
    downloaded_files = download_prefix_from_s3(s3_client, bucket_name, prefix="tests/generic", local_dir="./downloads/dbt-sql/")
    print("Downloaded files:")
    for p in downloaded_files:
        print(" ", p)
