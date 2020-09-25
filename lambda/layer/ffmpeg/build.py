import requests
import tarfile
import os
import re
import shutil
import logging
from zipfile import ZipFile
from os.path import basename
import boto3
import sys

from config import (
    LOG_LEVEL,
    BUILD_PATH,
    BIN_PATH,
    BINARY_URL,
    BINARY_ARCHIVE_PATH,
    AWS_REGION,
    BUCKET_NAME,
    BUCKET_PREFIX,
    PACKAGE_FILE,
)

if LOG_LEVEL == "DEBUG":
    import traceback
    import inspect
else:
    pass

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger()


def main() -> None:
    """ Creates Lambda layer for ffmpeg static """

    get_ffmpeg_binary(overwrite_bin=False)
    package(clean_bin=False)
    object_url = upload_to_s3()

    logger.info("Lambda layer created:")
    logger.info(object_url)


def binary_exists(bin_path: str = BIN_PATH) -> bool:
    """ Test if ffmpeg static binary already exists """

    assert_check({bin_path: str})

    if os.path.exists(f"{bin_path}/ffmpeg"):
        return True
    else:
        return False


def binary_folder(build_path: str = BUILD_PATH) -> str:
    """ Returns guessed ffmpeg directory name """

    assert_check({build_path: str})

    pattern = re.compile("^ffmpeg-git-([0-9]+)-amd64-static$")
    target_path = os.listdir(build_path)

    for target in target_path:
        if pattern.match(target):
            return target
        else:
            continue

    return False


def start_build_env(
    build_path: str = BUILD_PATH, bin_path: str = BIN_PATH
) -> bool:
    """ Creates build directory """

    assert_check({build_path: str, bin_path: str})

    try:
        if not os.path.exists(build_path):
            os.mkdir(build_path)
            logger.debug(f'Created directory: "{build_path}".')
        else:
            logger.debug(f'"{build_path}" exists.')

        if not os.path.exists(bin_path):
            os.mkdir(bin_path)
            logger.debug(f'Created directory: "{bin_path}".')
        else:
            logger.debug(f'"{bin_path}" exists.')

        return True
    except Exception as e:
        logger.error(e)
        raise

    return False


def terminate_build_env(build_path: str = BUILD_PATH) -> bool:
    """ Delete _build directory """

    assert_check({build_path: str})

    try:
        if os.path.exists(build_path):
            logger.info("Cleaning up...")
            shutil.rmtree(build_path)
            logger.info("...done")
        else:
            logger.warning(f"{build_path} does not exist, skipping deletion.")

        return True
    except Exception as e:
        logger.error(e)
        raise

    return False


def get_ffmpeg_binary(
    binary_url: str = BINARY_URL,
    binary_path: str = BINARY_ARCHIVE_PATH,
    build_path: str = BUILD_PATH,
    bin_path: str = BIN_PATH,
    overwrite_bin: bool = False,
) -> bool:
    """ Get static binary of ffmpeg """

    assert_check(
        {
            binary_url: str,
            binary_path: str,
            build_path: str,
            bin_path: str,
            overwrite_bin: bool,
        }
    )

    try:
        start_build_env()

        if os.path.exists(f"{bin_path}/ffmpeg"):
            if not overwrite_bin:
                logger.info(
                    "ffmpeg binary already exists, skipping fetch operation."
                )
                return True
            else:
                logger.warning(
                    "ffmpeg binary already exists and will be replaced."
                )
        else:
            logger.debug(f'"{bin_path}/ffmpeg" does not exist.')

        logger.info("fetching ffmpeg archive...")
        ffmpeg_archive = requests.get(binary_url)
        with open(binary_path, "wb") as w:
            w.write(bytes(ffmpeg_archive.content))
        logger.info("...done")

        logger.info("extracting archive...")
        with tarfile.open(binary_path) as f:
            f.extractall(f"{build_path}/")
        logger.info("...done")

        logger.info("Copying binary...")
        binary = f"{build_path}/{binary_folder()}/ffmpeg"
        shutil.copyfile(binary, "./bin/ffmpeg")
        os.chmod("./bin/ffmpeg", 0o777)
        logger.info("...done")

        terminate_build_env()

        return True
    except Exception as e:
        logger.error(e)
        raise

    return False


def package(
    bin_path: str = BIN_PATH,
    package_file: str = PACKAGE_FILE,
    clean_bin: bool = False,
) -> bool:
    """ Creates Lambda layer package """

    assert_check({bin_path: str, package_file: str, clean_bin: bool})

    try:
        logger.info("Creating package...")
        bin_file = f"{bin_path}/ffmpeg"

        with ZipFile(package_file, "w") as w:
            w.write(bin_file, arcname=bin_file, compresslevel=9)
        logger.info("...done")

        if clean_bin:
            logger.info("Cleaning binary...")
            shutil.rmtree(bin_path)
            logger.info("done...")
        else:
            logger.debug("Not cleaning binary.")

        logger.info(
            f"Package created successfully: 'ffmpeg_lambda_layer.zip'."
        )

        return True
    except Exception as e:
        logger.error(e)
        raise

    return False


def upload_to_s3(
    package_file: str = PACKAGE_FILE,
    aws_region: str = AWS_REGION,
    bucket_name: str = BUCKET_NAME,
    bucket_prefix: str = BUCKET_PREFIX,
) -> str:
    """
    Upload package to S3
    Returns S3 URL of object
    """

    assert_check(
        {
            package_file: str,
            aws_region: str,
            bucket_name: str,
            bucket_prefix: str,
        }
    )

    try:
        logger.info("uploading package...")
        s3 = boto3.client("s3", region_name=aws_region)
        key = basename(package_file)

        with open(package_file, "rb") as data:
            response = s3.upload_fileobj(
                data, bucket_name, f"{bucket_prefix}/{key}"
            )  # to do: enable versioning on S3

        logger.info("done...")
        logger.debug(response)

        url = f"https://s3-{aws_region}.amazonaws.com/{bucket_name}/{bucket_prefix}/{key}"

        return url
    except Exception as e:
        logger.error(e)
        raise

    return False


def assert_check(args: dict = None, log_level: str = LOG_LEVEL) -> bool:
    """ assert caller function args """

    if args is None:
        logger.critical("Arguments dict is empty or does not exist!")
        return False
    else:
        logging.debug("Args dictionary exists, processing assertion check...")

    try:
        for k, v in args.items():
            assert k is not None
            assert k != ""
            assert k != []
            assert k != {}
            assert k != ()
            assert type(k) == v

        return True
    except AssertionError:
        if log_level == "DEBUG":
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb)
            tb_info = traceback.extract_tb(tb)
            _, line, _func, text = tb_info[-1]
            logging.error(
                f'An error occurred on line {line} in statement "{text}" in function "{inspect.stack()[1].function}".'
            )
            return False
        else:
            logging.critical(
                f'An assertion error occured but did not call traceback because log level is: "{log_level}".'
            )
            return False
    except Exception as e:
        logging.error(e)
        raise

    return False


if __name__ == "__main__":
    main()
