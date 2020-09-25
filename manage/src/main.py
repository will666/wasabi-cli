import os
from tabulate import tabulate
import time
from init import logger, statistics, cloud_video_encoder_list
from constants import LOG_DISPLAY_ENV_VARS, MEDIA_ENCODE_PLATFORM
from s3 import (
    get_s3_files,
    create_s3_bucket,
    send_to_bucket,
    s3_clean,
    medias_copy,
)
from build_media import build_media_objects, build_card_objects
from db import create_table, seed_db_table
from helpers import is_filtered, export_to_json
from local import (
    get_local_medias_files,
    build_media_files_from_list,
    read_list_from_file,
)
from media_queue import create_queue, queue_count
from media_generator import remote_video_encoder, save_defer_encoding


def main(display_env: str = LOG_DISPLAY_ENV_VARS) -> None:
    """ Fetch, build and store S3 media files into DynamoDB """

    if display_env == "True":
        logger.debug("## Environment variables")
        logger.debug(os.environ)
    else:
        pass

    logger.debug("- Start of execution -")

    prepare_local_resources()
    setup_cloud_resources()
    hydrate_cloud_resources()

    logger.debug("- End of execution -")
    print(tabulate(statistics))

    monitor_remote_ops()

    logger.info("- All tasks executed successfully -")


def prepare_local_resources(
    media_encode_platform: str = MEDIA_ENCODE_PLATFORM,
) -> None:
    local_files = get_local_medias_files()
    build_media_files_from_list(local_files)
    if len(cloud_video_encoder_list) > 0 and media_encode_platform == "cloud":
        save_defer_encoding(cloud_video_encoder_list)


def setup_cloud_resources() -> None:
    create_s3_bucket()
    create_table()
    create_queue()
    # time.sleep(5)  # some rope for cloud resources creation


def hydrate_cloud_resources(
    media_encode_platform: str = MEDIA_ENCODE_PLATFORM,
) -> None:
    # media_sync()
    medias_copy()
    s3_clean()
    if len(cloud_video_encoder_list) > 0 and media_encode_platform == "cloud":
        remote_video_encoder()
    get_s3_files()
    data = read_list_from_file()
    medias = build_media_objects(data)
    cards = build_card_objects(medias)
    export_to_json(cards)
    seed_db_table(cards)


def monitor_remote_ops() -> None:
    """ Display number of running SQS task(s) """

    while queue_count() > 0:
        logger.info(f"Numbere of task(s) processing remotely: {queue_count()}")
        time.sleep(5)
        if queue_count() < 1:
            break


if __name__ == "__main__":
    main()
