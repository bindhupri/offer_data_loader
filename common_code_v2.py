from offer_bank.spark_config.spark import get_spark
from pyspark.sql.functions import explode, to_timestamp, lit, col, concat_ws, flatten, when
from pyspark.sql import functions as F
from pandas import json_normalize
from pytz import timezone
from datetime import datetime
from itertools import islice
from pyspark.sql.types import StringType, DoubleType, LongType, ArrayType
from offer_bank.blob_storage.blob_operations import read_from_blob_v2
from offer_bank.exceptions.big_query_exception import BigQueryException
from offer_bank.exceptions.blob_read_exception import BlobReadException
from offer_bank.utility import constants
import os
import sys
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.types import DataType, StructType, StructField, ArrayType, IntegerType, StringType, BooleanType, MapType
import json
import re

spark = get_spark()
timestamp_format = constants.TIMESTAMP_FORMAT
time_format = constants.TIME_FORMAT

ET = timezone('US/Eastern')
CT = timezone('US/Central')
MT = timezone('US/Mountain')
PT = timezone('US/Pacific')

us_timezones = {'CST': CT, 'CDT': CT,
              'EST': ET, 'EDT': ET,
              'MST': MT, 'MDT': MT,
              'PST': PT, 'PDT': PT}


def list_to_string(list_):
    return str(list_).strip('[]')


def process_event(event):
    data = [event]

    schema = StructType() \
            .add("payload", StructType() \
                .add("eventTimeStamp", StringType(),True) \
                .add("status", StringType(),True) \
                .add("isUpdate", BooleanType(),True) \
                .add("promotionNumber", StringType(),True) \
                .add("promotionName", StringType(),True) \
                .add("startDate", StringType(),True) \
                .add("endDate", StringType(),True) \
                .add("startTime", StringType(),True) \
                .add("endTime", StringType(),True) \
                .add("timeZone", StringType(),True) \
                .add("created", StringType(),True) \
                .add("lastModified", StringType(),True) \
                .add("name", StringType(),True) \
                .add("createdBy", StringType(),True) \
                .add("modifiedBy", StringType(),True) \
                .add("multiMatchType", StringType(),True) \
                .add("channels", ArrayType(StringType()),True) \
                .add("labels", ArrayType(StringType()),True) \
                .add("awardList", ArrayType(StructType() \
                    .add("promotionItemNumber", StringType(), True) \
                    .add("gs1Code", StringType(), True) \
                    .add("fundingPercent", StructType() \
                        .add("vendorFundingPercent", StringType(), True) \
                        .add("memberFundingPercent", StringType(), True) \
                        .add("samsFundingPercent", StringType(), True) \
                        , True) \
                    .add("awardType", StringType(), True) \
                    .add("value", StringType(), True) \
                    .add("discountMethod", StringType(), True) \
                    .add("discountLimit", StringType(), True) \
                    .add("discountSortOrder", StringType(), True) \
                    .add("itemRefId", ArrayType(IntegerType()), True) \
                    .add("membershipDuration", StructType() \
                        .add("period", IntegerType(), True) \
                        .add("periodUnit", StringType(), True) \
                        , True) \
                    , True) \
                , True) \
                .add("itemDetails", StructType() \
                    .add("itemListCount", IntegerType(),True) \
                    .add("itemsPayload", ArrayType(StructType() \
                        .add("itemType", StringType(), True) \
                        .add("mpq", StringType(), True) \
                        .add("itemListType", StringType(), True) \
                        .add("itemList", ArrayType(StringType()), True) \
                        .add("itemBigListURL", StringType(), True) \
                        .add("refId", IntegerType(), True) \
                        , True) \
                    , True) \
                , True) \
                .add("clubDetails", StructType() \
                    .add("clubListCount", IntegerType(),True) \
                    .add("clubsPayload", ArrayType(StructType() \
                        .add("clubType", StringType(), True) \
                        .add("clubList", ArrayType(StringType()), True) \
                        .add("clubBigListURL", StringType(), True) \
                        .add("clubInclusion", BooleanType(), True) \
                        , True) \
                    , True) \
                    .add("clubOverride", ArrayType(StructType() \
                        .add("clubNumber", StringType(), True) \
                        .add("startDate", StringType(), True) \
                        .add("startTime", StringType(), True) \
                        .add("endDate", StringType(), True) \
                        .add("endTime", StringType(), True) \
                        .add("timeZone", StringType(), True) \
                        , True) \
                    , True) \
                , True) \
                .add("memberDetails", StructType() \
                    .add("memberListCount", IntegerType(),True) \
                    .add("membersPayload", ArrayType(StructType() \
                        .add("memberType", StringType(), True) \
                        .add("memberList", ArrayType(StringType()), True) \
                        .add("memberBigListURL", StringType(), True) \
                        .add("isMemberInclusion", BooleanType(), True) \
                        , True) \
                    , True) \
                , True) \
                .add("membershipType", ArrayType(StringType()),True) \
                .add("offerSource", StringType(),True) \
            , True)

    event_df = spark.createDataFrame(data=data,schema=schema)
    return event_df


def query_cdp_items_table():
    try:
        bigquery_df = spark.read.format("bigquery").option("parentProject","stage-sams-offerbank")\
            .option("project","prod-sams-cdp")\
            .option("table","prod-sams-cdp.US_SAMS_PRODUCT360_CDP_VM.ITEM_GRP")\
            .load()
        return bigquery_df
    except Exception as e:
        raise BigQueryException(e)


def filter_tire_items(items_df):
    bigquery_df = query_cdp_items_table()
    item_catalog_df = bigquery_df.select(col("PROD_ID"), col("MDSE_CATG_CD"), col("MDSE_SUB_CATG_CD"), explode(col("ITEM_ID_ARRAY")).alias("ITEM_ID"))
    items = item_catalog_df.filter(item_catalog_df.MDSE_CATG_CD == 50).select(col("ITEM_ID"))
    filtered_items_df = items_df.select('item_number').subtract(items.select('ITEM_ID'))
    filtered_items_df = items_df.join(filtered_items_df, on='item_number', how='inner')
    print("Filtered Tyre items from the list:")
    filtered_items_df.show()

    return filtered_items_df


def check_event_expiry(event):
    print('Checking for event expiry..')
    end_datetime = event["payload"]["endDate"] + " " + event["payload"]["endTime"]
    timezone = event["payload"]["timeZone"]

    end_datetime = datetime.strptime(end_datetime, time_format)

    curent_datetime_given_timezone = datetime.now(us_timezones[timezone])
    curent_datetime_given_timezone = datetime.strftime(curent_datetime_given_timezone, time_format)
    curent_datetime_given_timezone = datetime.strptime(curent_datetime_given_timezone, time_format)

    print(f'End datetime of offer = {end_datetime}')
    print(f'Current datetime = {curent_datetime_given_timezone}')
    is_expired = curent_datetime_given_timezone > end_datetime
    print(f'Is offer expired : {is_expired}' )
    return is_expired


def chunks(data, size=25000):
    it = iter(data)
    for _ in range(0, len(data), size):
        yield {k:data[k] for k in islice(it, size)}

def transform_map_to_batch(key_value_pairs, size=25000):
    key_value_pairs_batch = []
    for pairs in chunks(key_value_pairs, size):
        key_value_pairs_batch.append(pairs)
    return key_value_pairs_batch


def query_mdp_member_table():
    try:
        if (os.environ["ENV"] == 'Stage' or os.environ["ENV"] == 'Dev'):
            sql = """SELECT * FROM `stage-sams-offerbank.stage_sams_offerbank.membership_id_uuid_mapping`"""
            member_details_df = spark.read.format("bigquery").option("parentProject","stage-sams-offerbank").option("project","stage-sams-offerbank").option("query", sql).load()
            return member_details_df
        else:
            sql = """SELECT `cardNumber`, `membershipId` FROM `prod-sams-mdp.mdp_secure.SAMS_UNIFIED_MEMBER_CARD`
                WHERE `current_ind` = 'Y' AND `card_current_ind` = 'Y' AND (TRIM(`cardStatus`) = 'ACTIVE' OR TRIM(`cardStatus`) = 'PICKUP' OR TRIM(`cardStatus`) = 'EXPIRED') AND `cardType` = 'MEMBERSHIPACCOUNT' """
            member_details_df = spark.read.format("bigquery").option("parentProject","stage-sams-offerbank").option("project","prod-sams-mdp").option("query", sql).load()
            return member_details_df
    except Exception as e:
        raise BigQueryException(e)


def get_membership_uuid_from_membership_id(members_df, offer_source):
    if ("membership_uuid" in members_df.columns):
        return members_df

    try:
        member_details_df = query_mdp_member_table()
    except BigQueryException as e:
        raise BigQueryException(e)

    members_df = members_df.alias('member_df')
    member_details_df = member_details_df.alias('details_df')
    if(offer_source.casefold() == "Tetris".casefold()):
        member_uuid_df = member_details_df.join(members_df, member_details_df.cardNumber == members_df.membership_id)\
                    .select('details_df.cardNumber','details_df.membershipId','member_df.propensity')
        """Dropping duplicate membership uuids"""
        member_uuid_df = member_uuid_df.orderBy(F.col("propensity").desc()).dropDuplicates(["membershipId"])
    else:
        member_uuid_df = member_details_df.join(members_df, member_details_df.cardNumber == members_df.membership_id)\
                    .select('details_df.cardNumber','details_df.membershipId')
        """Dropping duplicate membership uuids"""
        member_uuid_df = member_uuid_df.dropDuplicates(["membershipId"])

    member_uuid_df = member_uuid_df.withColumnRenamed("cardNumber","membership_id") \
                    .withColumnRenamed("membershipId","membership_uuid")

    print("Filtered uuid from MDP ")

    return member_uuid_df


def create_offer_df(event_df):
    offers_df = event_df.select(
        col("payload.promotionNumber").cast('long').alias("offer_id"),
        col("payload.status").alias("status"),
#         col("offerSetId").alias("offer_set_id"),
        to_timestamp(event_df.payload.startDate, timestamp_format).alias("start_datetime"),
        to_timestamp(event_df.payload.endDate, timestamp_format).alias("end_datetime"),
        to_timestamp(event_df.payload.eventTimeStamp, timestamp_format).alias("event_timestamp"),
        col("payload.timeZone").alias("time_zone"),
        col("payload.offerSource").alias("offer_source"),
        col("payload.awardList.discountMethod").alias("discount_type"),
        col("payload.awardList.value").alias("discount_value"),
        col("payload.channels").alias("applicable_channel"),
        flatten(col("payload.clubDetails.clubsPayload.clubList")).alias("club_list"),
        col("payload.memberDetails.membersPayload.memberBigListURL").alias("member_list_location"),
#         col("bigListName").alias("member_list_name"),
        col("payload.memberShipType").alias("membership_type"),
        col("payload.itemDetails.itemsPayload.itemBigListURL").alias("item_list_location"),
        col("payload.labels").alias("labels"))

    offers_df = offers_df.withColumn("offer_set_id", when(lit(True), lit("")))
    offers_df = offers_df.withColumn("campaign_id", when(lit(True), lit("")))
    offers_df = offers_df.withColumn("campaign_id", when(col("campaign_id").isNotNull(), col("campaign_id")).cast(LongType()))
    offers_df = offers_df.withColumn("campaign_name", when(lit(True), lit("")))
    offers_df = offers_df.withColumn("club_list", when(col("club_list").isNotNull(), col("club_list")).cast(ArrayType(LongType())))
    offers_df = offers_df.withColumn('offer_source', when(col('offer_source').isNotNull(), concat_ws(',', 'offer_source')))
    offers_df = offers_df.withColumn('discount_type', when(col('discount_type').isNotNull(), concat_ws(',', 'discount_type')))
    offers_df = offers_df.withColumn('discount_value', when(col('discount_value').isNotNull(), concat_ws(',', 'discount_value').cast(DoubleType())))
    offers_df = offers_df.withColumn('member_list_location', when(col('member_list_location').isNotNull(), concat_ws(',', 'member_list_location')).otherwise(lit("")))
    offers_df = offers_df.withColumn('member_list_name', when(lit(True), lit("")))
    offers_df = offers_df.withColumn('item_list_location', when(col('item_list_location').isNotNull(), concat_ws(',', 'item_list_location')).otherwise(lit("")))

    return offers_df


def create_offer_item_df(event_df):
    item_schema = StructType() \
            .add("item_number", LongType(),True) \
            .add("item_type", StringType(),True)

    items_df = spark.createDataFrame([], schema=item_schema)

    offer_id = event_df.first()['payload']['promotionNumber']
    item_list = []
    if event_df.first()['payload']['itemDetails'] and event_df.first()['payload']['itemDetails']['itemsPayload']:
        for item in event_df.first()['payload']['itemDetails']['itemsPayload']:
            if item['refId'] in event_df.first()['payload']['awardList'][0]['itemRefId']:

                if(item['itemListType'].casefold() == "list".casefold()):
                    for item_number in item['itemList']:
                        if item['itemType'] == constants.DISCOUNTED_ITEMS:
                            item_list.append((item_number, constants.DISCOUNTED_ITEM))
                        if item['itemType'] == constants.ELIGIBLE_ITEMS:
                            item_list.append((item_number, constants.ELIGIBLE_ITEM))
                    new_items = spark.createDataFrame(item_list, ["item_number", "item_type"])
                    items_df = items_df.union(new_items)
                    item_list = []
                else:
                    item_list_url = item['itemBigListURL']
                    offer_source = event_df.first()['payload']['offerSource']
                    try:
                        new_items = read_from_blob_v2(item_list_url, offer_source, constants.ITEM_BLOB)
                        new_items = new_items.drop("Propensity")
                    except Exception as e:
                        raise BlobReadException(constants.ITEM_BLOB, item_list_url, e)
                    if(offer_source.casefold() == 'broadreach' or offer_source.casefold() == 'tio'):
                        new_items = new_items.toDF("ItemNumber")
                    new_items = new_items.withColumnRenamed("ItemNumber", "item_number")
                    if item['itemType'] == constants.DISCOUNTED_ITEMS:
                        new_items = new_items.withColumn("item_type", lit(constants.DISCOUNTED_ITEM))
                    if item['itemType'] == constants.ELIGIBLE_ITEMS:
                        new_items = new_items.withColumn("item_type", lit(constants.ELIGIBLE_ITEM))
                    items_df = items_df.union(new_items)

    items_df = filter_tire_items(items_df)

    """Adding offer_id and product_id to the items_df"""
    items_df = items_df.withColumn("item_number",col("item_number").cast(LongType()))
    offer_item_df = items_df.withColumn("offer_id", lit(offer_id).cast(IntegerType()))
    offer_item_df = offer_item_df.withColumn("product_id", when(lit(True), lit("")))

    return items_df, offer_item_df


def create_club_override_df(event_df):
    pattern = re.compile(r'^(?:(?:([01]?\d|2[0-3]):)?([0-5]?\d):)?([0-5]?\d)$')

    offer_id = event_df.first()['payload']['promotionNumber']
    club_override_list = []
    if event_df.first()['payload']['clubDetails']['clubOverride']:
        for club_override in event_df.first()['payload']['clubDetails']['clubOverride']:
            club_override_list.append((club_override['clubNumber'], club_override['startDate'] + ' ' + ("00:00:00" if not bool(pattern.match(club_override['startTime'])) else club_override['startTime']), club_override['endDate'] + ' ' + ("23:59:59" if not bool(pattern.match(club_override['endTime'])) else club_override['endTime']), club_override['timeZone']))
        club_override_df = spark.createDataFrame(club_override_list, ["club_number", "start_datetime", "end_datetime", "time_zone"])
        club_override_df = club_override_df.withColumn("club_number",col("club_number").cast(LongType()))
        club_override_df = club_override_df.withColumn("start_datetime", to_timestamp("start_datetime", timestamp_format))
        club_override_df = club_override_df.withColumn("end_datetime", to_timestamp("end_datetime", timestamp_format))
        club_override_df = club_override_df.withColumn("offer_id", lit(offer_id).cast(IntegerType()))
    else:
        club_override_df = spark.createDataFrame([], StructType([]))

    return club_override_df


def query_club_finder_table():
    try:
        if(os.environ["ENV"] == 'Stage' or os.environ["ENV"] == 'Dev'):
            sql = """SELECT DISTINCT id as club_id, address.state FROM `sams-clubfinder-nonprod.clubfinder.clubdata` where isActive = true and (clubType IS NULL OR clubType = 'REGULAR')"""
            club_details_df = spark.read.format("bigquery").option("parentProject","stage-sams-offerbank").option("project","sams-clubfinder-nonprod").option("query", sql).load()
        else:
            sql = """SELECT DISTINCT id as club_id, address.state FROM `sams-clubfinder-prod.clubfinder.clubdata` where isActive = true and (clubType IS NULL OR clubType = 'REGULAR')"""
            club_details_df = spark.read.format("bigquery").option("parentProject","stage-sams-offerbank").option("project","sams-clubfinder-prod").option("query", sql).load()
        return club_details_df
    except Exception as e:
        raise BigQueryException(e)


def resolve_club_list(event, offer_source):
    applicable_channel = event["payload"]["channels"]

    if "clubDetails" in event["payload"] and "clubsPayload" in event["payload"]["clubDetails"]:
        for club_payload in event["payload"]["clubDetails"]["clubsPayload"]:
            club_list_location = club_payload["clubBigListURL"]
            club_biglist_name = club_payload["bigListName"]
            if "clubType" in club_payload:
                club_list_type = club_payload["clubType"]
            else:
                club_list_type = ""
            club_payload["clubList"]
            is_club_excluded = not club_payload["clubInclusion"]

            """Fetch the clubs from club list location if the type is biglist"""
            if club_list_type.casefold() == "biglist":
                print("Fetching the clubs from club list location")
                try:
                    club_list_df = read_from_blob_v2(club_list_location, offer_source, constants.CLUB_BLOB)
                except Exception as e:
                    raise BlobReadException(constants.CLUB_BLOB, club_list_location, e)
                print("Successfully fetched clubs from club list location")
                club_list_df = club_list_df.select(col("_c0"))
                club_list = club_list_df.rdd.flatMap(lambda x: x).collect()
                club_list = list(map(int, club_list))
                if is_club_excluded == True:
                    print("Fetching all the club id's from bigquery table:")
                    try:
                        all_club_list_df = query_club_finder_table()
                    except BigQueryException as e:
                        raise BigQueryException(e)
                    all_club_list_df = all_club_list_df.select(col("club_id"))
                    print("Successfully processed clubs from bigquery table")
                    all_club_list = all_club_list_df.rdd.flatMap(lambda x: x).collect()
                    all_club_list = list(map(int, all_club_list))
                    print(f"Excluding the clubs from the clublist - {club_list}")
                    club_list = [x for x in all_club_list if x not in club_list]
                club_payload["clubList"] = club_list

            elif club_list_type.casefold() == "standard":
                print("Fetching all the club id's from bigquery table:")
                try:
                    club_list_df = query_club_finder_table()
                except BigQueryException as e:
                    raise BigQueryException(e)
                if club_biglist_name == "ALL":
                    club_list_df = club_list_df.select(col("club_id"))
                elif club_biglist_name == "STANDARD CLUBS - PUERTO RICO ONLY":
                    club_list_df = club_list_df.filter(col("state") == "PR").select(col("club_id"))
                elif club_biglist_name == "STANDARD CLUBS - US EXCLUDING PUERTO RICO":
                    club_list_df = club_list_df.filter(col("state") != "PR").select(col("club_id"))
                print("Successfully processed clubs from bigquery table")
                club_list = club_list_df.rdd.flatMap(lambda x: x).collect()
                club_list = list(map(int, club_list))
                club_payload["clubList"] = club_list

    elif ['D2H'] == applicable_channel:
        print("Defaulting the offer to online club")
        event["payload"].setdefault("clubDetails", {}).setdefault("clubsPayload", []).append({"clubList": [6279]})

    return event
