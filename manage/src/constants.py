import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env", override=True)

LOCAL_MEDIA_PATH = os.getenv("LOCAL_MEDIA_PATH", "/tmp/input")
LOCAL_MEDIA_OUTPUT_PATH = os.getenv("LOCAL_MEDIA_OUTPUT_PATH", "/tmp/output")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_PATH = os.getenv("LOG_PATH", "../logs")
LOG_CLEAR = os.getenv("LOG_CLEAR", False).capitalize()
LOG_DISPLAY_ENV_VARS = os.getenv("LOG_DISPLAY_ENV_VARS", False).capitalize()
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
TABLE_READ_CAPACITY_UNITS = int(os.getenv("TABLE_READ_CAPACITY_UNITS", 5))
TABLE_WRITE_CAPACITY_UNITS = int(os.getenv("TABLE_WRITE_CAPACITY_UNITS", 5))
FILES_LIST_PATH = os.getenv("FILES_LIST_PATH", "/tmp")
FILES_LIST_FILENAME = os.getenv("FILES_LIST_FILENAME", "s3_media_list.txt")
CONFIG_PATH = os.getenv("CONFIG_PATH", "../config")
OUTPUT_IMAGE_WIDTH = int(os.getenv("OUTPUT_IMAGE_WIDTH", 430))
OUTPUT_IMAGE_HEIGHT = int(os.getenv("OUTPUT_IMAGE_HEIGHT", 400))
S3_PREFIX = os.getenv("S3_PREFIX")
VIDEO_ENCODE = bool(os.getenv("VIDEO_ENCODE", False).capitalize())
VIDEO_PRESETS = os.getenv("VIDEO_PRESETS", "web.mp4")
ENCODER_THREADS = int(os.getenv("ENCODER_THREADS", 1))
##
MEDIA_ENCODE_PLATFORM = os.getenv(
    "MEDIA_ENCODE_PLATFORM", "cloud"
)  # cloud|local
QUEUE_NAME = os.getenv("QUEUE_NAME", "liamvalentin-video-encode")
QUEUE_VISIBILITY = os.getenv("QUEUE_VISIBILITY", "900")
