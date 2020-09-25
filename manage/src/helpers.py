from init import logger, filter_list, statistics
from datetime import datetime
from constants import CONFIG_PATH, FILES_LIST_PATH, LOG_LEVEL
import re
import json
import sys

if LOG_LEVEL == "DEBUG":
    import traceback
    import inspect
else:
    pass

SUPPORTED_PICTURE_FORMATS = [
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
]
SUPPORTED_MOVIE_FORMATS = [
    ".mov",
    ".m4v",
    ".mp4",
    ".MP4",
    ".ogg",
    ".mpg",
    ".mpeg",
]


def get_media_type(
    file_name: str = None,
    supported_pictures_formats: tuple = SUPPORTED_PICTURE_FORMATS,
    supported_movies_formats: tuple = SUPPORTED_MOVIE_FORMATS,
) -> str:
    """ Returns the media type of the input filename """

    pictures = [(item, item.upper()) for item in supported_pictures_formats]
    movies = [(item, item.upper()) for item in supported_movies_formats]

    if list(filter(file_name.endswith, pictures)) != []:
        media_type = "picture"
    elif list(filter(file_name.endswith, movies)) != []:
        media_type = "movie"
    else:
        media_type = "UNSUPPORTED"
        logger.warning(f"Type unsupported: \"{file_name.split('.')[-1]}\".")

    return media_type


def media_ts_format(ts: str = None, media: str = None) -> str:
    """ invoked by build_media_files_from_list() - check and format 'ts' """

    ts_month = ts.split(" ")[0]

    if len(ts_month) == 3:  # if month is "Dec" (3 letters)"
        try:
            media_ts = (
                datetime.strptime(ts, "%b %d, %Y").date().strftime("%Y%m%d")
            )
            return media_ts
        except Exception as e:
            logger.warning(
                f'"{media}" ts format is not supported. Skipping it.'
            )
            logger.debug(f"message was: {e}")
            return False
    elif len(ts_month) > 3:  # if month is "December"
        try:
            media_ts = (
                datetime.strptime(ts, "%B %d, %Y").date().strftime("%Y%m%d")
            )
            return media_ts
        except Exception as e:
            logger.warning(
                f'"{media}" ts format is not supported. Skipping it.'
            )
            logger.debug(f"message was: {e}")
            return False
    else:
        return False


def is_filtered(
    target: str = None,
    config_path: str = CONFIG_PATH,
    filter_list: list = filter_list,
) -> bool:
    """ Returns True if target is a valid file format """

    if target and type(target) == str and len(filter_list) > 0:
        pattern = re.compile("^[*][.][a-z-A-Z-0-9]+$")
        wildcard_filter = [
            item.split(".")[-1] for item in filter_list if pattern.match(item)
        ]

        filename_filter = [
            item for item in filter_list if not pattern.match(item)
        ]

        if (
            target in filename_filter
            or target.split(".")[-1].lower() in wildcard_filter
            or target.split(".")[-1].upper() in wildcard_filter
        ):
            return True
        else:
            return False
    else:
        return False


def export_to_json(
    db_data: list = None,
    display: bool = False,
    files_list_path: str = FILES_LIST_PATH,
) -> bool:
    """ Export DB objects to JSON """

    try:
        data = json.dumps(db_data, indent=4)
        data_count = len(json.loads(data))

        statistics.append(["export_to_json", data_count])

        with open(f"{files_list_path}/cards.json", "w") as f:
            f.write(data)
        logger.info(
            f"DB objects exported to JSON file successfully: {files_list_path}/cards.json"
        )
        if display:
            print(data)
        else:
            pass

    except Exception as e:
        logger.error(e)
        raise

    return True


# def assert_check(args: dict = None, log_level: str = LOG_LEVEL) -> bool:
#     """ assert caller function args """

#     if args is None:
#         logger.critical("Arguments dict is empty or does not exist!")
#         return False
#     elif log_level != "DEBUG":
#         return False
#     else:
#         logger.debug("Args dictionary exists, processing assertion check...")

#     try:
#         for k, v in args.items():
#             assert k is not None
#             assert k != ""
#             assert k != []
#             assert k != {}
#             assert k != ()
#             if v == 'list':
#                 assert type(k) == list
#             elif v == 'dict':
#                 assert type(k) == dict
#             elif v == 'tuple':
#                 assert type(k) == tuple
#             elif v == 'str':
#                 assert type(k) == str
#             elif v == 'int':
#                 assert type(k) == int
#             elif v == 'bool':
#                 assert type(k) == bool
#             elif v == 'float':
#                 assert type(k) == float
#             else:
#                 pass
#             # assert type(k) == v
#         logger.debug("Assertion check: OK")
#     except AssertionError:
#         if log_level == "DEBUG":
#             _, _, tb = sys.exc_info()
#             traceback.print_tb(tb)
#             tb_info = traceback.extract_tb(tb)
#             _, line, _func, text = tb_info[-1]
#             logger.error(
#                 f'An error occurred on line {line} in statement "{text}" in function "{inspect.stack()[1].function}".'
#             )
#             return False
#         else:
#             logger.critical(
#                 f'An assertion error occured but did not call traceback because log level is: "{log_level}".'
#             )
#             return False
#     except Exception as e:
#         logger.error(e)
#         raise

#     return True
