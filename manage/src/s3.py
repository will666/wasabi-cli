import boto3
import os
import re
import json
import subprocess
from os.path import basename
from constants import (
    BUCKET_NAME,
    FILES_LIST_PATH,
    FILES_LIST_FILENAME,
    AWS_REGION,
    LOCAL_MEDIA_OUTPUT_PATH,
    S3_PREFIX,
    LOG_PATH,
    CONFIG_PATH,
    MEDIA_ENCODE_PLATFORM,
    VIDEO_ENCODE,
)
from init import logger, statistics
from local import get_local_medias_files
from helpers import get_media_type


def send_to_bucket(
    media_file: str,
    ts: str,
    bucket_name: str = BUCKET_NAME,
    s3_prefix: str = S3_PREFIX,
    aws_region: str = AWS_REGION,
) -> bool:
    """ Send file to S3 """

    logger.info(f'Sending "{media_file}" to bucket "{bucket_name}"...')

    try:
        key = f"{s3_prefix}/{ts}/{basename(media_file)}"
        s3 = boto3.client("s3", region_name=aws_region)
        with open(media_file, "rb") as data:
            s3.upload_fileobj(data, bucket_name, key)
        logger.debug(f"media_file: {media_file} - key: {key}")
    except Exception as e:
        logger.error(e)
        raise

    logger.info(f'File "{media_file}" sent successfully to bucket: "{key}"')

    return True


def get_s3_files(
    bucket_name: str = BUCKET_NAME,
    save_to_disk: bool = True,
    files_list_path: str = FILES_LIST_PATH,
    files_list_filename: str = FILES_LIST_FILENAME,
    aws_region: str = AWS_REGION,
    s3_prefix: str = S3_PREFIX,
) -> list:
    """ Get S3 objects and creates list """

    logger.info("Building media list from S3 objects...")
    logger.debug(
        f"Context Parameters: {get_s3_files.__name__} => {get_s3_files.__code__.co_varnames}"
    )

    data = []

    # testing format: assets/20160823/img.jpg
    pattern = re.compile(
        "^[a-z-A-Z-0-9]+/[a-z-A-Z-0-9]+/[0-9]{8}/.+[.][a-z-A-Z-0-9]+$"
    )

    try:
        s3 = boto3.client("s3", region_name=aws_region)
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)

        for page in pages:
            for obj in page["Contents"]:
                if pattern.match(obj["Key"]):
                    data.append(obj["Key"])
                else:
                    logger.warning(
                        f'Wrong filename format, object "{obj["Key"]}", not added to the list.'
                    )

        statistics.append(["get_s3_files", len(data)])

        logger.info("Media Objects list generated successfully.")
        logger.debug(f"Media objects count: {len(data)}.")

        if save_to_disk:
            logger.info("Writing media list to disk...")
            export_data = [f"{item}\n" for item in data]
            with open(f"{files_list_path}/{files_list_filename}", "w") as w:
                w.writelines(export_data)
            logger.info(
                f'List successfully saved to disk: "{files_list_path}/{files_list_filename}".'
            )
        else:
            pass
    except Exception as e:
        logger.error(e)
        raise

    return data


def create_s3_bucket(
    bucket_name: str = BUCKET_NAME, aws_region: str = AWS_REGION
) -> bool:
    """ Create the S3 bucket of the project """

    s3 = boto3.client("s3", region_name=aws_region)
    bucket_exists = True

    try:
        response = response = s3.list_buckets()
        buckets = [
            bucket["Name"]
            for bucket in response["Buckets"]
            if bucket["Name"] == bucket_name
        ]

        if len(buckets) > 0:
            logger.warning(
                "S3 bucket already exists. Skipping bucket creation."
            )
            bucket_exists = True
        else:
            bucket_exists = False
    except Exception as e:
        logger.error(e)
        raise

    if not bucket_exists:
        try:
            response = s3.create_bucket(
                Bucket=bucket_name,
                ACL="private",
                CreateBucketConfiguration={"LocationConstraint": aws_region},
            )
            logger.info(f'Created S3 bucket "{bucket_name}" successfully.')
            logger.debug(f"S3 client response: {response}")
        except Exception as e:
            logger.error(e)
            raise
    else:
        return False

    return True


def media_sync(
    local_path: str = LOCAL_MEDIA_OUTPUT_PATH,
    bucket_name: str = BUCKET_NAME,
    remote_path_prefix: str = S3_PREFIX,
    log_path: str = LOG_PATH,
    aws_region: str = AWS_REGION,
    config_path: str = CONFIG_PATH,
) -> bool:
    """ Synchronize local/S3 media files tree """

    exclude_s3_file_path = f"{config_path}/exclude_s3.txt"
    if os.path.exists(exclude_s3_file_path):
        with open(exclude_s3_file_path, "r") as r:
            common_oses_filter = r.read().splitlines()
        cli_filter_args = ""
        cli_filter_args = cli_filter_args.join(
            [
                f' --exclude "{item}"'
                for item in common_oses_filter
                if not item.startswith("#") or item != "\n"
            ]
        )
    else:
        cli_filter_args = ""

    logger.info("Starting sync...")
    logger.info(f"S3 sync task log => tail -F {log_path}/s3_sync.log")

    try:
        cli_cmd = f"aws s3 sync {local_path}/ s3://{bucket_name}/{remote_path_prefix}/ --delete --region {aws_region} {cli_filter_args}"
        logger.debug(f"cli command: {cli_cmd}")
        with open(f"{log_path}/s3_sync.log", "w") as w:
            proc = subprocess.run(
                cli_cmd,
                shell=True,
                check=True,
                stdout=w,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )

        if proc.returncode == 0:
            with open(f"{log_path}/s3_sync.log", "r") as r:
                processed_objects = r.read().splitlines()

            processed_objects = [
                item for item in processed_objects if "upload" in item
            ]
            statistics.append(["media_sync", len(processed_objects)])

            logger.info("Sync completed successfully.")
            logger.debug(
                f"{len(processed_objects)} files have been synchronized successfully."
            )
            logger.debug(f"S3 CLI returned code: {proc.returncode} => OK")
        else:
            logger.critical("Something wrong happened during sync operation!")
            return False
    except Exception as e:
        logger.error(e)
        raise

    return True


def s3_clean(
    bucket_name: str = BUCKET_NAME, aws_region: str = AWS_REGION
) -> bool:
    """ Delete imcomplete multi-part uploads """

    logger.info("Getting list of incomplete uploads...")
    try:
        multipart_uploads_cmd = f"aws s3api list-multipart-uploads --bucket {bucket_name} --region {aws_region}"
        proc = subprocess.run(
            multipart_uploads_cmd,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
        logger.debug(
            f'"s3api list-multipart-uploads" returns code: {proc.returncode} => OK'
        )
    except Exception as e:
        if "exit status 254" in str(e):
            logger.warning(
                f"Bucket {bucket_name} does not exist. Stopping here."
            )
            return False
        else:
            logger.error(e)
            raise

    if proc.returncode == 0 and proc.stdout:
        multipart_uploads_list = proc.stdout.strip()
        multipart_uploads_list = json.loads(multipart_uploads_list)["Uploads"]
        logger.info("Delete in progess...")
        try:
            for item in multipart_uploads_list:
                proc_cmd = f"aws s3api abort-multipart-upload --bucket {bucket_name} --region {aws_region} --key \"{item['Key']}\" --upload-id {item['UploadId']}"

                proc = subprocess.run(
                    proc_cmd,
                    shell=True,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                )

                statistics.append(["s3_clean", len(multipart_uploads_list)])
                logger.debug(
                    f'"s3api abort-multipart-upload" returns code: {proc.returncode} => OK'
                )
                logger.info(f"Deleted incomplete upload: \"{item['Key']}\".")
            logger.debug(
                f"{len(multipart_uploads_list)} incomplete upload(s) deleted."
            )
        except Exception as e:
            logger.error(e)
            raise

        return True
    else:
        logger.info("Nothing to clean.")
        return True


# replacement of media_sync ?
def medias_copy(
    local_path: str = LOCAL_MEDIA_OUTPUT_PATH,
    video_encode: bool = VIDEO_ENCODE,
    media_encode_platform: str = MEDIA_ENCODE_PLATFORM,
) -> bool:
    """ Copy media files to S3 """

    logger.info("Starting copy...")

    try:
        medias = get_local_medias_files(path=local_path, save_to_disk=False)
        logger.debug(medias)
        for media in medias:
            media_type = get_media_type(basename(media))
            ts = media.split("/")[-2]

            if media_type == "movie":
                if video_encode == "True" and media_encode_platform == "cloud":
                    send_to_bucket(media, ts)
                elif (
                    video_encode == "True" and media_encode_platform == "local"
                ):
                    logger.info(
                        f"Skipping copy of {media} for local re-encoding."
                    )
            elif media_type == "picture":
                send_to_bucket(media, ts)
            else:
                logger.warning(f"Media type is: {media_type} !")

        logger.info(
            f"{len(medias)} medias files have been successfully copied."
        )
    except Exception as e:
        logger.error(e)
        raise

    statistics.append(["medias_copy", len(medias)])

    logger.info("...done.")

    return True
