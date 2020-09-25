import logging
import subprocess
import os
import shutil
import boto3
import json
from os.path import basename
from boto3.dynamodb.conditions import Key

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
WORKDIR = os.getenv("WORKDIR", "/tmp")
BINARY_NAME = os.getenv("BINARY_NAME", "ffmpeg")
VIDEO_FORMAT = os.getenv("VIDEO_FORMAT", "mp4")
VIDEO_CODEC = os.getenv("VIDEO_CODEC", "h264")
AUDIO_CODEC = os.getenv("AUDIO_CODEC", "aac")
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "1000K")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128K")
THREADS = os.getenv("THREADS", "1")
SRC_S3_BUCKET = os.getenv("SRC_S3_BUCKET", "liamvalentin.com-test")
DST_S3_BUCKET = os.getenv("DST_S3_BUCKET", SRC_S3_BUCKET)
DST_S3_PREFIX = os.getenv("S3_PREFIX", "assets")
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
QUEUE_NAME = os.getenv("QUEUE_NAME", "liamvalentin-video-encode")
TABLE_NAME = os.getenv("TABLE_NAME", "liamvalentin-data-test")

if LOG_LEVEL == "DEBUG":
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

logger = logging.getLogger()
logger.setLevel(log_level)

s3 = boto3.client("s3", region_name=AWS_REGION)
sqs = boto3.client("sqs", region_name=AWS_REGION)
queues = sqs.list_queues(QueueNamePrefix=QUEUE_NAME)
queue_url = queues["QueueUrls"][0]
db = boto3.client("dynamodb", region_name=AWS_REGION)


def lambda_handler(event, context) -> None:
    logger.info("EVENT: " + json.dumps(event))

    receipt_handle = event["Records"][0]["receiptHandle"]
    event_data = event["Records"][0]["body"]
    key = event_data["src"]
    ts = event_data["ts"]
    delete_old = bool(event_data["delete_old"])
    src_name = basename(key)
    dst_name = os.path.splitext(basename(key))[0]

    src_lambda_storage = f"{WORKDIR}/{src_name}"
    dst_lambda_storage = f"{WORKDIR}/{dst_name}.{VIDEO_FORMAT}"
    dst_s3_storage = f"{DST_S3_PREFIX}/{ts}/{dst_name}.{VIDEO_FORMAT}"

    setup_layer()
    download_s3_object(key)
    encode_src(src_lambda_storage, dst_lambda_storage)
    upload_s3_object(dst_lambda_storage, dst_s3_storage)
    delete_queue_message(receipt_handle)

    if delete_old:
        delete_s3_object(key)


def setup_layer(
    workdir: str = WORKDIR, binary_name: str = BINARY_NAME
) -> bool:
    """ Setup Lambda layer """

    logger.info("Starting copy of binary...")
    try:
        ffmpeg_bin = f"{workdir}/{binary_name}"
        shutil.copyfile(f"/opt/bin/{binary_name}", ffmpeg_bin)
        os.chmod(ffmpeg_bin, 0o777)
    except Exception as e:
        logger.error(e)
        raise

    logger.info("...done")

    return True


def encode_src(
    src: str,
    dst: str,
    workdir: str = WORKDIR,
    binary_name: str = BINARY_NAME,
    video_format: str = VIDEO_FORMAT,
    video_codec: str = VIDEO_CODEC,
    audio_codec: str = AUDIO_CODEC,
    video_bitrate: str = VIDEO_BITRATE,
    audio_bitrate: str = AUDIO_BITRATE,
    threads: str = THREADS,
) -> bool:
    """ Re-encode file """

    assert src is not None, "Assertion error: 'src' is None"
    assert type(src) == str, "Assertion error: wrong type for 'src'"
    assert dst is not None, "Assertion error: 'dst' is None"
    assert type(dst) == str, "Assertion error: wrong type for 'dst'"

    logging.info(f"Starting re-encoding file: {src}...")

    try:
        cli_cmd = f"{workdir}/{binary_name} -loglevel quiet -i '{src}' -f {video_format} -vcodec {video_codec} -acodec {audio_codec} -vb {video_bitrate} -ab {audio_bitrate} -threads {threads} -y '{dst}'"
        subprocess.call(
            cli_cmd,
            shell=True,
            # check=True,
        )
    except Exception as e:
        logging.error(e)
        raise

    logging.info("...done")
    return True


def upload_s3_object(
    target: str, dst_filename: str, dest_bucket: str = DST_S3_BUCKET
) -> bool:
    """
    Upload S3 event's object
    Returns the uploaded file
    """

    assert target is not None, "Assertion error: 'target' is None"
    assert type(target) == str, "Assertion error: wrong type for 'target'"
    assert dst_filename is not None, "Assertion error: 'dst_filename' is None"
    assert (
        type(dst_filename) == str
    ), "Assertion error: wrong type for 'dst_filename'"

    logger.info(f"Starting upload of {target}...")
    try:
        with open(target, "rb") as data:
            s3.upload_fileobj(data, dest_bucket, dst_filename)
    except Exception as e:
        logging.error(e)
        raise

    logging.info("...done")
    return True


def download_s3_object(
    key: str, src_s3_bucket: str = SRC_S3_BUCKET, dest_dir: str = WORKDIR
) -> bool:
    """
    Download S3 event's object
    Returns of the downloaded file
    """

    assert key is not None, "Assertion error: 'key' is None"
    assert type(key) == str, "Assertion error: wrong type for 'key'"

    logger.info(f"Starting download of {key}...")
    try:
        filename = basename(key)
        lambda_storage = f"{dest_dir}/{filename}"

        with open(lambda_storage, "wb") as data:
            response = s3.download_fileobj(src_s3_bucket, key, data)

        logger.info("...done")
    except Exception as e:
        logging.error(e)
        raise

    logging.info("...done")
    logger.debug(response)

    return True


def delete_queue_message(
    receipt_handle: str, queue_url: str = queue_url
) -> bool:
    """ Delete SQS queue message """

    assert receipt_handle is not None
    assert type(receipt_handle) == str

    logger.debug("Starting deletion of queue message...")

    try:
        response = sqs.delete_message(
            QueueUrl=queue_url, ReceiptHandle=receipt_handle
        )
        logging.debug(response)
    except Exception as e:
        logging.error(e)
        raise

    logger.debug(f"Successfully deleted message {receipt_handle} from queue.")

    return True


def delete_s3_object(key: str, bucket_name: str = DST_S3_BUCKET) -> bool:
    """ Delete S3 object """

    assert key is not None
    assert type(key) == str

    try:
        response = s3.delete_object(Bucket=bucket_name, Key=key)
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    return True
