import os
import re
import shutil
import subprocess
from constants import (
    VIDEO_ENCODE,
    LOG_PATH,
    VIDEO_PRESETS,
    ENCODER_THREADS,
    S3_PREFIX,
    MEDIA_ENCODE_PLATFORM,
    FILES_LIST_PATH,
)
from init import (
    logger,
    video_preset_data,
    statistics,
    cloud_video_encoder_list,
)
from PIL import Image
from helpers import get_media_type
from media_queue import send_to_queue
import json


def media_generate(
    media: str = None,
    output_path: str = None,
    media_ts: str = None,
    output_image_width: int = None,
    output_image_height: int = None,
    processed_files_count: int = None,
    unprocessed_files: list = None,
    video_encode: str = VIDEO_ENCODE,
    log_path: str = LOG_PATH,
    s3_prefix: str = S3_PREFIX,
    media_encode_platform: str = MEDIA_ENCODE_PLATFORM,
) -> list:
    """ invoked by build_media_files_from_list() - gemerates media files """

    media_name = media.split("/")[-1]
    media_type = get_media_type(media_name)

    if not os.path.exists(f"{output_path}/{media_ts}"):
        os.mkdir(f"{output_path}/{media_ts}")
        logger.debug(f'Created directory: "{output_path}/{media_ts}".')
    else:
        pass

    if media_type == "picture":
        logger.info(
            f"Picture type identified, starting generation of media..."
        )
        image = Image.open(media)
        if image:
            with image as im:
                im.thumbnail((output_image_width, output_image_height))
                im.save(
                    f"{output_path}/{media_ts}/{media_name}",
                    format="JPEG",
                    quality="web_high",
                    dpi=(72, 72),
                )
            processed_files_count += 1
            logger.info(
                f'Generated media: "{output_path}/{media_ts}/{media_name}".'
            )
        else:
            logger.warning(
                f'Impossible to open the image file: "{media_name}"! File identified format is : "{media_type}". Skipping it.'
            )
    elif media_type == "movie":
        if video_encode:
            logger.info(f"Movie type identified...")

            if media_encode_platform == "local":
                if os.path.exists(f"{log_path}/ffmpeg.log"):
                    with open(f"{log_path}/ffmpeg.log", "r+") as w:
                        w.truncate(0)
                else:
                    pass
                video_encoder(media, media_ts, output_path)
            elif media_encode_platform == "cloud":
                logger.info(f"Movie type identified, starting copy of file...")
                shutil.copyfile(
                    media, f"{output_path}/{media_ts}/{media_name}"
                )
                logger.info(
                    f'File copied successfully: "{media}" => "{output_path}/{media_ts}/{media_name}"'
                )
                movie = f"{s3_prefix}/{media_ts}/{media_name}"
                cloud_video_encoder_list.append(
                    {"src": movie, "ts": media_ts, "delete_old": True}
                )
                logger.info(
                    f"Added movie '{movie}' to queue for defered remote re-encoding."
                )
            else:
                logger.critical(
                    'Wrong or missing value! Valid values for "media_encode_platform": local|cloud'
                )
        else:
            logger.info(f"Movie type identified, starting copy of file...")
            shutil.copyfile(media, f"{output_path}/{media_ts}/{media_name}")
            logger.info(
                f'File copied successfully: "{media}" => "{output_path}/{media_ts}/{media_name}"'
            )

        processed_files_count += 1
    else:
        unprocessed_files.append(media)
        logger.warning(f'Impossible to process file: "{media}". Skipping it.')

    return (processed_files_count, unprocessed_files)


def save_defer_encoding(
    movies_list: list, files_list_path: str = FILES_LIST_PATH
) -> bool:
    """ Store list of defered remote video encoding to file """

    assert movies_list is not None
    assert type(movies_list) == list

    try:
        data = json.dumps(movies_list, indent=4)
        file_path = f"{files_list_path}/defered_encode.json"
        with open(file_path, "w") as w:
            w.write(data)
    except Exception as e:
        logger.error(e)
        raise

    logger.info(f"Defered encoding list saved successfully: '{file_path}'")
    return True


def video_encoder(
    media: str = None,
    ts: str = None,
    output_path: str = None,
    log_path: str = LOG_PATH,
    video_preset_data: dict = video_preset_data,
    media_presets: str = VIDEO_PRESETS,
    encoder_threads: int = ENCODER_THREADS,
) -> bool:
    """ Encode video media based on preset """

    try:
        i = 0
        media_presets = media_presets.split(" ")

        for media_preset in media_presets:
            media_preset = media_preset.split(".")
            preset_category = media_preset[0]
            preset_format = media_preset[1]
            settings = video_preset_data[preset_category][preset_format]

            logger.debug(f"Settings: {settings}")

            file_format = settings["format"]
            vcodec = settings["vcodec"]
            acodec = settings["acodec"]
            video_bitrate = settings["video_bitrate"]
            audio_bitrate = settings["audio_bitrate"]
            encoder_threads = int(encoder_threads)

            i += 1
            logger.info(
                f'Encoding media "{media}" using preset "{preset_category} -> {preset_format}" ...'
            )
            logger.info(
                f'Processing task(s) for: "{preset_format}"... {i}/{len(media_presets)}.'
            )

            output_filename = (
                f"{media.split('/')[-1].split('.')[-2]}.{preset_format}"
            )
            output_file = f"{output_path}/{ts}/{output_filename}"

            cli_cmd = f"ffmpeg -i '{media}' -f {file_format} -vcodec {vcodec} -acodec {acodec} -vb {video_bitrate} -ab {audio_bitrate} -threads {encoder_threads} -y '{output_file}'"
            logger.debug(f"cli command: {cli_cmd}")

            with open(f"{log_path}/ffmpeg.log", "a") as w:
                subprocess.run(
                    cli_cmd,
                    shell=True,
                    check=True,
                    stdout=w,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                )
    except Exception as e:
        logger.error(e)
        raise

    return True


def remote_video_encoder(files_list_path: str = FILES_LIST_PATH) -> bool:
    """ Send movies list to SQS -> lambda/ffmpeg """

    logger.info("Starting remote movie re-encoding operations...")

    data_path = f"{files_list_path}/defered_encode.json"
    if os.path.exists(data_path):
        try:
            with open(data_path, "r") as r:
                movies = r.read()
            for movie in movies:
                queue_message = send_to_queue(movies)
                logger.info(f"Re-encoding process launched for '{movie}'.")
                logger.debug(queue_message)
        except Exception as e:
            logger.error(e)
            raise
    else:
        logger.critical(f"Path does not exist: '{data_path}'. Stopping here.")
        return False

    logger.info("...done.")

    return True
