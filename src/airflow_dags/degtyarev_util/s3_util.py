from json import dumps
from typing import List

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def save_data_to_s3(data: dict, prefix: str, filename_source_key: str = 'id') -> None:
    """Store string data on S3."""
    s3hook = S3Hook()
    key = f'{prefix}{data[filename_source_key]}.json'
    try:
        s3hook.load_string(string_data=dumps(data), key=key)
    except ValueError as err:
        print(err)


def list_s3_keys(s3_prefix: str) -> List[str]:
    """Return list of S3 keys (without bucket name and s3:// prefix)."""
    bucket, key = S3Hook.parse_s3_url(s3_prefix)
    s3hook = S3Hook()
    keys = s3hook.list_keys(
        bucket_name=bucket,
        prefix=key,
        delimiter='/'
    )

    return keys
