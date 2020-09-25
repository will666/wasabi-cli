import os
import logging
import boto3
from boto3.dynamodb.conditions import Key
import re
from os.path import basename

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
TABLE_NAME = os.getenv("TABLE_NAME", "")
AWS_REGION = os.getenv("AWS_REGION", "us-west-2")

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event: dict = None, context: dict = None) -> bool:
    """ Update DynamoDB following S3 events (ObjectCreated/ObjectRemoved) """

    logger.debug("## ENVIRONMENT VARIABLES")
    logger.debug(os.environ)
    logger.debug("## EVENT")
    logger.debug(event)

    event = event["Records"][0]
    bucket_name = event["s3"]["bucket"]["name"]
    event_name = event["eventName"]
    root_key = event["s3"]["object"]["key"]
    key = root_key.split("/")

    name = key[-1]
    root_ts = key[-2]
    ts = f"{root_ts[0:4]}-{root_ts[4:6]}-{root_ts[6:8]}"

    path = f"{key[0]}/{key[1]}/{key[2]}"
    url = f"https://s3-{AWS_REGION}.amazonaws.com/{bucket_name}/{path}/{name}"

    supported_pictures_formats = [
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
    ]
    supported_movies_formats = [
        ".mov",
        ".m4v",
        ".mp4",
        ".ogg",
        ".mpg",
        ".mpeg",
    ]

    pictures = [(item, item.upper()) for item in supported_pictures_formats]
    movies = [(item, item.upper()) for item in supported_movies_formats]

    # picture_types = [item for item in supported_pictures_formats]
    # picture_types_list = picture_types.extend([item.upper() for item in supported_pictures_formats])
    # movie_types = [item for item in supported_movies_formats]
    # movie_types_list = movie_types.extend([item.upper() for item in supported_pictures_formats])

    if list(filter(name.endswith, pictures)) != []:
        kind = "picture"
    elif list(filter(name.endswith, movies)) != []:
        kind = "movie"
    else:
        kind = "folder"

    logger.info("*** CONTEXT ***")
    logger.info(f"The S3 event name is: {event_name}")
    logger.info(f"Timestamp is: {ts}")
    logger.info(f"Object kind is: {kind}")
    logger.info(f"Object name is: {name}")
    logger.info(f"Object prefix is: {path}")
    logger.info(f"Object S3 url is: {url}")

    if "ObjectCreated" in event_name:
        if kind == "folder":
            logger.debug("## CREATE CARD")
            create_card(ts)
            logger.info("Card successfully added to DB.")
            return True
        else:
            logger.debug("## GET CARD BY ts")
            response = get_card_by_ts(ts)
            logger.debug(response)
            if response["Items"]:
                logger.info(f"DB record exists for ts: {ts}")
                logger.debug("## UPDATE ADD MEDIA ITEM")
                update_add_media_item(response, ts, name, path, url, kind)
                logger.info("Object successfully added to DB.")
                return True
            else:
                logging.info("Creating card...")
                create_card(ts)
                logger.info("Card successfully added to DB.")
                update_add_media_item(response, ts, name, path, url, kind)
                logger.info("Object successfully added to DB.")

                return True
    elif "ObjectRemoved" in event_name:
        pattern = re.compile("^[0-9]{8}$")
        is_ts = pattern.match(basename(root_key))

        if kind == "folder" or is_ts:
            logger.debug("## DELETE CARD")
            delete_card(ts)
            logger.info("Card successfully removed from DB.")
            return True
        else:
            logger.debug("## GET CARD BY ts")
            response = get_card_by_ts(ts)
            logger.debug(response)
            if response["Items"]:
                logger.info(f"DB record exists for ts: {ts}")
                logger.debug("## UPDATE DELETE MEDIA ITEM")
                update_del_media_item(response, ts, name)
                logger.info("Object successfully removed from DB.")
                return True
            else:
                delete_card(ts)
                # logger.warning(f'Response does not contain object "Items"')
                # return False
    else:
        logger.warning(f'Event: "{event_name}" not supported! Stopping here')
        return False

    return False


# assets/20160720/DSCN0206.JPG


def get_card_by_ts(ts: str = None) -> dict:
    """ Get cards by 'ts' """

    try:
        logger.debug("## Response - get_card_by_ts")
        response = table.query(KeyConditionExpression=Key("ts").eq(ts))
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    return response


def create_card(ts: str = None) -> bool:
    """ Create a card """

    item = {}
    item["ts"] = ts
    item["medias"] = []

    try:
        logger.debug("## Response - [create_card]")
        response = table.put_item(Item=item)
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    return True


def delete_card(ts: str = None) -> bool:
    """ Delete card """

    try:
        logger.debug("## Response - [delete_card]")
        response = table.delete_item(Key={"ts": ts})
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    return True


def update_add_media_item(
    response: dict = None,
    ts: str = None,
    name: str = None,
    path: str = None,
    url: str = None,
    kind: str = None,
) -> bool:
    """ Insert media in table item's map """

    medias = response["Items"][0]["medias"]
    media_name = None

    try:
        media_name = [item["name"] for item in medias if item["name"] == name][
            0
        ]
    except Exception as e:
        if "IndexError" or "list index out of range" in str(e):
            logger.debug(e)
            pass
        else:
            logger.error(e)
            raise

    if media_name is not None:
        logger.warning(
            f'Item "{media_name}" exists in DB - [update_add_media_item] **'
        )
        logger.debug(response)
        return True
    else:
        try:
            response = table.update_item(
                Key={"ts": ts},
                UpdateExpression=f"set medias = list_append(medias, :media)",
                ExpressionAttributeValues={
                    ":media": [
                        {"name": name, "path": path, "url": url, "kind": kind}
                    ]
                },
                ReturnValues="UPDATED_NEW",
            )
            logger.debug("## Response - [update_add_media_item]")
            logger.debug(response)
        except Exception as e:
            logger.error(e)
            raise

    return True


def update_del_media_item(
    response: dict = None, ts: str = None, name: str = None
) -> bool:
    """ Delete a media from table item's map """

    medias = response["Items"][0]["medias"]

    try:
        index = [
            idx for (idx, key) in enumerate(medias) if key["name"] == name
        ][0]
        logger.debug(
            f"## List of indexes to delete: {index} - [update_del_media_item]"
        )
    except Exception as e:
        logger.error(e)
        raise

    try:
        response = table.update_item(
            Key={"ts": ts},
            UpdateExpression=f"remove medias[{index}]",
            ReturnValues="ALL_NEW",
        )
        logger.debug("## Response - [update_del_media_item]")
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        raise

    return True
