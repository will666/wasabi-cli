import os

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
BUILD_PATH = os.getenv("BUILD_PATH", "./_build")
BIN_PATH = os.getenv("BIN_PATH", "./bin")
BINARY_URL = os.getenv(
    "BINARY_URL",
    "https://johnvansickle.com/ffmpeg/builds/ffmpeg-git-amd64-static.tar.xz",
)
BINARY_ARCHIVE_PATH = os.getenv(
    "BINARY_ARCHIVE_PATH", f"{BUILD_PATH}/ffmpeg-git-amd64-static.tar.xz"
)
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
BUCKET_NAME = os.getenv("BUCKET_NAME", "liamvalentin.com")
BUCKET_PREFIX = os.getenv("BUCKET_PREFIX", "tmp/lambda_layer")
PACKAGE_FILE = os.getenv("PACKAGE_FILE", "./package/ffmpeg_lambda_layer.zip")
