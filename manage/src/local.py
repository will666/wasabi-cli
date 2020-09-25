from constants import (
    LOCAL_MEDIA_PATH,
    FILES_LIST_PATH,
    FILES_LIST_FILENAME,
    CONFIG_PATH,
    OUTPUT_IMAGE_WIDTH,
    OUTPUT_IMAGE_HEIGHT,
    LOCAL_MEDIA_OUTPUT_PATH,
    LOG_PATH,
)
from init import logger, statistics
from helpers import is_filtered, media_ts_format, get_media_type
from media_generator import media_generate, video_encoder
import os
import re


def get_local_medias_files(
    path: str = LOCAL_MEDIA_PATH,
    save_to_disk: bool = True,
    files_list_path: str = FILES_LIST_PATH,
    files_list_filename: str = FILES_LIST_FILENAME,
    config_path: str = CONFIG_PATH,
) -> list:
    """ Generates a list of local media files """

    if os.path.exists(path):
        local_medias = []
        filtered_files = []

        try:
            logger.info("Generating list of local files...")
            for dirpath, _, files in os.walk(path):
                for filename in files:
                    fname = os.path.join(dirpath, filename)
                    if is_filtered(filename):
                        filtered_files.append(fname)
                    else:
                        local_medias.append(fname)

            if len(local_medias) > 0:
                statistics.append(
                    ["get_local_medias_files", len(local_medias)]
                )

                logger.info("List successfully generated.")
                logger.debug(f"Count: {len(local_medias)} local files.")
            else:
                logger.critical(
                    f'No files found in source directory: "{path}".'
                )
                return False

            if save_to_disk:
                logger.info("Writing local files list to disk...")
                data_to_write = [item + "\n" for item in local_medias]

                with open(
                    f"{files_list_path}/{files_list_filename}", "w"
                ) as w:
                    w.writelines(data_to_write)

                logger.info(
                    f'The list has been saved successfully: "{files_list_path}/{files_list_filename}".'
                )
            else:
                pass

            if len(filtered_files) > 0:
                logger.info(
                    f'Number of file(s) excluded by filter specified in "{config_path}/exclude_local.txt": {len(filtered_files)}.'
                )
                logger.debug(f"excluded by filter: {filtered_files}")
            else:
                pass
        except Exception as e:
            logger.error(e)
            raise
        return local_medias
    else:
        logger.critical(f'Missing input "path"! Stopping here!')
        return False


def build_media_files_from_list(
    local_files_list: list = None,
    output_image_width: int = OUTPUT_IMAGE_WIDTH,
    output_image_height: int = OUTPUT_IMAGE_HEIGHT,
    output_path: str = LOCAL_MEDIA_OUTPUT_PATH,
    log_path: str = LOG_PATH,
) -> bool:
    """ Generates web friendly resized images and copy other media files """

    logger.info("Generating web friendly images...")
    processed_files_count = 0
    unprocessed_files = []
    path_pattern = re.compile("^.*?/[0-9]{8}/.*[.][a-z-A-Z-0-9]+$")
    ts_pattern = re.compile("^[0-9]{8}$")

    try:
        for media in local_files_list:
            ts = media.split("/")[-2]

            if path_pattern.match(media):
                media_ts = ts
            elif not ts_pattern.match(ts):
                media_ts = media_ts_format(ts, media)
            else:
                logger.warning(
                    f'The file path format should by like eg.: "path/ts/image.jpg".'
                )
                logger.critical(
                    f'Input file path format "{media}" is incorrect! Stopping here!'
                )
                return False

            if not media_ts:
                unprocessed_files.append(media)
                logger.warning(
                    f"Could not identify the date format. Skipping."
                )
            else:
                gen = media_generate(
                    media=media,
                    output_path=output_path,
                    media_ts=media_ts,
                    output_image_width=output_image_width,
                    output_image_height=output_image_height,
                    processed_files_count=processed_files_count,
                    unprocessed_files=unprocessed_files,
                )
                # processed_files_count = gen[0]
                # unprocessed_files = gen[1]
                processed_files_count, unprocessed_files = gen

        statistics.append(
            ["build_media_files_from_list", processed_files_count]
        )
        logger.info(
            f"{processed_files_count} images have been generated successfully."
        )

        log_file = f"{log_path}/unprocessed_files.log"

        if len(unprocessed_files) > 0:
            up_files = [item + "\n" for item in unprocessed_files]

            with open(log_file, "w") as w:
                w.writelines(up_files)

            logger.warning(f"{len(unprocessed_files)} unprocessed file(s)!")
            logger.debug(f"Unprocessed file(s): {unprocessed_files}")
        elif os.path.exists(log_file):
            with open(log_file, "r+") as t:
                t.truncate(0)
        else:
            pass

        logger.info("Image files tree generation done.")

        if len(unprocessed_files) > 0:
            logger.info(
                f'Some files were not processed, please review the list: "{log_path}/unprocessed_files.log".'
            )
        else:
            pass
    except Exception as e:
        logger.error(e)
        raise

    return True


def read_list_from_file(
    files_list_path: str = FILES_LIST_PATH,
    files_list_filename: str = FILES_LIST_FILENAME,
) -> list:
    """ Import list from file """

    if os.path.exists(f"{files_list_path}/{files_list_filename}"):
        try:
            with open(f"{files_list_path}/{files_list_filename}", "r") as r:
                data = r.read().splitlines()

            statistics.append(["read_list_from_file", len(data)])
            logger.info(f"{len(data)} items imported from file.")
        except Exception as e:
            logger.error(e)
            raise
        return data
    else:
        logger.critical(
            f'Cannot open the file "{files_list_path}/{files_list_filename}", looks like it does not exists.'
        )
        return False

    logger.critical("Something went wrong!")
    return False


def process_local_movie_medias(
    local_media_output_path: str = LOCAL_MEDIA_OUTPUT_PATH,
    files_list_path: str = FILES_LIST_PATH,
    files_list_filename: str = FILES_LIST_FILENAME,
) -> bool:
    """ Get movie files """

    logger.info("Starting batch movie encoding...")
    try:
        with open(f"{files_list_path}/{files_list_filename}", "r") as r:
            data = r.read().splitlines()

        for item in data:
            if get_media_type(item) == "movie":
                ts = item.split("/")[-2]
                ts = media_ts_format(ts, item)
                logger.debug(ts)
                if ts:
                    video_encoder(
                        media=item, ts=ts, output_path=local_media_output_path
                    )
            else:
                pass
    except Exception as e:
        logger.error(e)
        raise

    logger.info("Encoder done.")

    return True
