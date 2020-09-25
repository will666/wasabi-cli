import boto3
from constants import (
    TABLE_NAME,
    TABLE_READ_CAPACITY_UNITS,
    TABLE_WRITE_CAPACITY_UNITS,
    AWS_REGION,
)
from init import logger, statistics


def create_table(
    table_name: str = TABLE_NAME,
    ReadCapacityUnits: int = TABLE_READ_CAPACITY_UNITS,
    WriteCapacityUnits: int = TABLE_WRITE_CAPACITY_UNITS,
    aws_region: str = AWS_REGION,
) -> bool:
    """ Creates DynamoB table """

    try:
        client = boto3.client("dynamodb", region_name=aws_region)
        response = client.list_tables()
        tables = [
            table for table in response["TableNames"] if table == table_name
        ]

        if len(tables) > 0:
            logger.warning(
                f'Table "{table_name}" already exists. Skipping table creation.'
            )
            return False
        else:
            logger.info(
                f'Table "{table_name}" does not exist. Starting creation process...'
            )
    except Exception as e:
        logger.error(e)
        raise

    logger.info("Creating DB table...")
    logger.debug(
        f"Context Parameters: {create_table.__name__} => {create_table.__code__.co_varnames}"
    )
    try:
        dynamodb = boto3.resource("dynamodb", region_name=aws_region)
        table = dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "ts", "AttributeType": "S"}
            ],
            KeySchema=[{"AttributeName": "ts", "KeyType": "HASH"}],
            ProvisionedThroughput={
                "ReadCapacityUnits": int(ReadCapacityUnits),
                "WriteCapacityUnits": int(WriteCapacityUnits),
            },
        )
        logger.info("Table created successfully.")
        logger.debug(table)
    except dynamodb.exceptions.ResourceInUseException as e:
        logger.warning(
            f'Table "{table_name}" already exists. Skipping table creation.'
        )
        logger.debug(e)
        return False

    return True


def seed_db_table(
    db_objects: list = None,
    table_name: str = TABLE_NAME,
    aws_region: str = AWS_REGION,
) -> bool:
    """ Insert DB objects into table """

    logger.info("Inserting data into DB...")
    logger.debug(
        f"Context Parameters: {seed_db_table.__name__} => {seed_db_table.__code__.co_varnames}"
    )

    try:
        dynamodb = boto3.resource("dynamodb", region_name=aws_region)
        table = dynamodb.Table(table_name)

        with table.batch_writer() as batch:
            for item in db_objects:
                batch.put_item(Item=item)

        statistics.append(["seed_db_table", len(db_objects)])

        logger.info(f"{len(db_objects)} item(s) were inserted in DB.")
    except Exception as e:
        logger.error(e)
        raise

    return True
