from pyspark.sql.functions import col, lit
import offer_bank.utility.common_code_v2 as common_code
from offer_bank.spark_config.spark import get_spark
from offer_bank.database.db_operations import insert_into_database, read_database
from offer_bank.database.query_db_v2 import delete_items, delete_offer, update_offer_details, delete_club_overrides, query_non_br_offer_details
import offer_bank.utility.map_creation as map_creation
import offer_bank.cache.offer_cache as offer_cache
from offer_bank.exceptions.cache_insertion_failed import CacheInsertionFailed
from offer_bank.exceptions.db_insertion_failed import DbInsertionFailed
from offer_bank.exceptions.blob_read_exception import BlobReadException
from offer_bank.exceptions.dataframe_transform_failed import DataframeTransformException
import offer_bank.bigquery.bigquery_operations as bq_operations
from offer_bank.exceptions.big_query_exception import BigQueryException
from offer_bank.utility import constants
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType, ArrayType
import sys

spark = get_spark()

def process_updated_offer_event(connection_pool, event, db_offer):
    offer_id = event["payload"]["promotionNumber"]

    if "tetris" in event["payload"]["labels"]:
        event["payload"]["offerSource"] = "TETRIS"
    elif "tio" in event["payload"]["labels"]:
        event["payload"]["offerSource"] = "TIO"
    else:
        event["payload"]["offerSource"] = "BROADREACH"
    offer_source = event["payload"]["offerSource"]

    if db_offer[0][1] != offer_source:
        print(f"Invalid update payload.")
        return f"Invalid update payload. Offer source {db_offer[0][1]} of offer {offer_id} cannot be updated once created."

    timezone = event["payload"]["timeZone"]

    """Preprocess date and time before creating dataframe"""
    event["payload"]["startDate"] = event["payload"]["startDate"] + " " + event["payload"]["startTime"]
    event["payload"]["endDate"] = event["payload"]["endDate"] + " " + event["payload"]["endTime"]
    event["payload"]["eventTimeStamp"] = event["payload"]["eventTimeStamp"][:19].replace("T", " ")

    """Null check for timezone and setting default value to CST"""
    if timezone == "":
        event["payload"]["timeZone"] = "CST"

    """Resolve club list data"""
    event = common_code.resolve_club_list(event, offer_source)

    """Process the incoming event as map to dataframe"""
    print("Process the incoming event as map to dataframe")
    try:
        event_df = common_code.process_event(event)
        print("-------------------------------------------Event DataFrame-------------------------------------------")
        event_df.printSchema()
#         event_df.show(truncate=False)
    except (Exception) as error:
        raise DataframeTransformException(error)

    """Generate offer and items dataframe to update into the db"""
    print("Generate offer and items dataframe to update into the db")
    offers_df = common_code.create_offer_df(event_df)
    print("-------------------------------------------offers_df DataFrame-------------------------------------------")
    offers_df.printSchema()
#     offers_df.show()
    try:
        items_df, offer_item_df = common_code.create_offer_item_df(event_df)
        print("-------------------------------------------items_df DataFrame-------------------------------------------")
        items_df.printSchema()
#         items_df.show()
        print("-------------------------------------------offer_item_df DataFrame-------------------------------------------")
        offer_item_df.printSchema()
#         offer_item_df.show()
    except BlobReadException as e:
            raise BlobReadException(constants.ITEM_BLOB, event_df.first()['offerAttributes_itemBigList'], e)

    club_override_df = common_code.create_club_override_df(event_df)
    print("-------------------------------------------club_override_df DataFrame-------------------------------------------")
    club_override_df.printSchema()
#     club_override_df.show()

    print("deleting old and inserting new items")
    delete_items(offer_id, connection_pool)

    """Delete items from BigQuery"""
    bq_operations.bigquery_delete_offer_items(offer_id)

    print("deleting old and inserting new items")
    delete_club_overrides(offer_id, connection_pool)

    """Delete items from BigQuery"""
    bq_operations.bigquery_delete_club_overrides(offer_id)

    """Delete offers from BigQuery"""
    bq_operations.bigquery_delete_offer(offer_id)

    """Update offers table"""
    offers_dict = offers_df.toPandas().to_dict('list')
    print("Updating offer in offers table in the database:")
    update_offer_details(offers_dict, connection_pool)

    try:
        bq_operations.insert_into_bigquery('offers', offers_df)
    except (Exception) as error:
        print(f"Error inserting data to BigQuery: {error}")
        raise BigQueryException(error)

    try:
        insert_into_database('offer_items_v2', offer_item_df)
    except (Exception) as error:
        print(f"Error inserting data to database: {error}")
        raise DbInsertionFailed(error)

    try:
        bq_operations.insert_into_bigquery('offer_items', offer_item_df)
    except (Exception) as error:
        print(f"Error inserting data to BigQuery: {error}")
        raise BigQueryException(error)

    if not club_override_df.rdd.isEmpty():
        try:
            insert_into_database('club_overrides', club_override_df)
        except (Exception) as error:
            print(f"Error inserting data to database: {error}")
            raise DbInsertionFailed(error)

        try:
            bq_operations.insert_into_bigquery('club_overrides', club_override_df)
        except (Exception) as error:
            print(f"Error inserting data to BigQuery: {error}")
            raise BigQueryException(error)


    """Update offer cache"""
    items_list = items_df.select('item_number').where(items_df.item_type == constants.DISCOUNTED_ITEM).rdd.flatMap(lambda x: x).collect()
    eligible_items_list = items_df.select('item_number').where(items_df.item_type == constants.ELIGIBLE_ITEM).rdd.flatMap(lambda x: x).collect()
    print("Updating offer details in offer cache:")
    offer_cache_map = map_creation.create_offer_cache_map_v2(event_df, club_override_df, offer_id, items_list, eligible_items_list)
    try:
        print('Inserting offer in cache :')
        offer_cache.insert_into_cache(offer_cache_map)
    except (Exception) as error:
        raise CacheInsertionFailed(constants.OFFER_CACHE)

    return f"Offer {offer_id} updated successfully."
