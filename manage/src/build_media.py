from constants import AWS_REGION, BUCKET_NAME
from init import logger, statistics
from helpers import get_media_type
from operator import itemgetter
import re
from collections import defaultdict


def build_media_objects(
    items: list = None,
    aws_region: str = AWS_REGION,
    bucket_name: str = BUCKET_NAME,
) -> list:
    """ Build media objects """

    mediaItems: list = []
    ts = None
    logger.info("Building media list dictionaries...")
    logger.debug(
        f"Context Parameters: {build_media_objects.__name__} => {build_media_objects.__code__.co_varnames}"
    )

    try:
        for item in items:
            key = item.split("/")
            name = key[3]
            ts = key[2]
            ts = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}"
            path = f"{key[0]}/{key[1]}/{key[2]}"
            url = f"https://s3-{aws_region}.amazonaws.com/{bucket_name}/{path}/{name}"

            media_type = get_media_type(name)

            if ts != "" and name != "":
                media = {}
                media["ts"] = ts
                media["name"] = name
                media["kind"] = media_type
                media["path"] = path
                media["url"] = url
                mediaItems.append(media)
            else:
                logger.warning(f"ts = {ts} and name = {name}. Stopping here.")
                return False

        data = sorted(mediaItems, key=itemgetter("ts"), reverse=False)

        nbr_data = len(data)
        nbr_items = len(items)

        statistics.append(["build_media_objects", len(data)])

        logger.info("Media list dictionaries built successfully.")
        logger.debug(f"{nbr_data} objects in media list.")

        if nbr_data != nbr_items:
            logger.critical(
                "Inconsistency found between data input and output! Stopping here!"
            )
            logger.debug(
                f"Input objects list count [{nbr_items}] and generated media objects count [{nbr_data}] are uneven. Stopping here."
            )
            return False
        else:
            pass
    except Exception as e:
        logger.error(e)
        raise

    return data


def build_card_objects(media_list: list = None) -> list:
    """ Creates DB objects from S3 objects list """

    logger.info("Crafting list of DB objects...")
    logger.debug(
        f"Context Parameters: {build_card_objects.__name__} => {build_card_objects.__code__.co_varnames}"
    )
    medias_list = defaultdict(list)
    try:
        for item in media_list:
            medias_list[item["ts"]].append(
                {
                    "name": item["name"],
                    "path": item["path"],
                    "url": item["url"],
                    "kind": item["kind"],
                }
            )
        medias = [{"ts": k, "medias": v} for k, v in medias_list.items()]

        statistics.append(["build_card_objects", len(medias)])

        logger.info(f'{len(medias)} "card" objects generated successfully.')
    except Exception as e:
        logger.error(e)
        raise

    return medias
