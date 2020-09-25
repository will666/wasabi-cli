import os
import json
from logger import init_logger
from PIL import ImageFile
from constants import (
    LOG_PATH,
    LOG_CLEAR,
    LOG_LEVEL,
    CONFIG_PATH,
    FILES_LIST_PATH,
)

if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)
elif not os.path.exists(FILES_LIST_PATH):
    os.mkdir(FILES_LIST_PATH)
else:
    pass

log_files = (
    f"{LOG_PATH}/app.log",
    f"{LOG_PATH}/app.warning.log",
    f"{LOG_PATH}/app.error.log",
    f"{LOG_PATH}/s3_sync.log",
    f"{LOG_PATH}/ffmpeg.log",
    f"{LOG_PATH}/unprocessed_files.log",
)

if LOG_LEVEL == "DEBUG":
    testing_mode = True
else:
    testing_mode = False

logger = init_logger(__name__, testing_mode=testing_mode)

if LOG_CLEAR == "True":
    try:
        for log_file in log_files:
            if os.path.exists(log_file):
                with open(log_file, "r+") as t:
                    t.truncate(0)
            else:
                pass
    except Exception as e:
        logger.error(e)
        raise
else:
    pass

try:
    exclude_list = f"{CONFIG_PATH}/exclude_local.txt"
    if os.path.exists(exclude_list):
        with open(exclude_list, "r") as r:
            filter_list = r.read().splitlines()
        filter_list = [
            item
            for item in filter_list
            if not item.startswith("#") or item != "\n"
        ]
    else:
        filter_list = None
except Exception as e:
    logger.error(e)
    raise

try:
    presets_file = f"{CONFIG_PATH}/encoder_presets.json"
    if os.path.exists(presets_file):
        with open(presets_file, "r") as r:
            video_preset_data = json.load(r)
    else:
        logger.warning(
            f"{presets_file} does not exist, could not load video presets!"
        )
except Exception as e:
    logger.error(e)
    raise

statistics = []
cloud_video_encoder_list = []

# allow corrupt picture files to be processed
ImageFile.LOAD_TRUNCATED_IMAGES = True
