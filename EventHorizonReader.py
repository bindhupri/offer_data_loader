import json
import psycopg2
from pydantic import BaseModel, Field, validator, root_validator, ValidationError
from typing import List
from datetime import datetime, timezone
from offer_bank.database.query_db_v2 import create_connection_pool, close_connection_pool, audit_offer_event, update_offer_event_status
from offer_bank.NavigatorV2 import navigate_event
from offer_bank.UpdateOfferMembership import process_update_memberlist_event
from offer_bank.utility.secrets import fetch_postgres_secrets
from offer_bank.utility.send_mail import send_failure_mail
from offer_bank.utility.send_teams_message import send_teams_failure_message
from offer_bank.exceptions.cache_insertion_failed import CacheInsertionFailed
from offer_bank.exceptions.db_connection_failed import DbConnectionFailed
from offer_bank.exceptions.secret_fetch_failed import SecretFetchFailed
from offer_bank.exceptions.db_insertion_failed import DbInsertionFailed
from offer_bank.exceptions.big_query_exception import BigQueryException
from offer_bank.exceptions.blob_read_exception import BlobReadException
from offer_bank.exceptions.dataframe_transform_failed import DataframeTransformException
import sys
import os
from offer_bank.utility import constants
import time

from asyncio import run
from datetime import timedelta
from eventhorizon import SendOptions, Event
from typing import List, Optional
from offer_bank.utility.secrets import fetch_event_horizon_secrets

from eventhorizon import (
    Client,
    ClientOptions,
    Credentials,
    Environment,
    SubscribeSyncOptions,
    EventPacket,
    Event,
    TimeoutException
)

print_bars = "######################################################################################################################################"

class ClubsPayload(BaseModel):
    clubType: str
    bigListName: str
    clubList: list
    clubBigListURL: Optional[str]
    clubInclusion: bool

    @root_validator()
    def validate_clubs_payload(cls,values):
        clubType = values.get("clubType")
        clubTypeSet = {"list", "biglist", "Standard"}
        clubList = values.get("clubList")
        clubBigListURL = values.get("clubBigListURL")

        if clubType not in clubTypeSet:
            raise ValueError(f"Club type must be in {clubTypeSet}, got '{clubType}'")
        elif clubType == "list" and not clubList:
            raise ValueError("Club list is empty")
        elif clubType == "biglist" and not clubBigListURL:
            raise ValueError("Club biglist url is empty")
        return values

class ClubDetails(BaseModel):
    clubListCount: int
    clubsPayload: Optional[List[ClubsPayload]]

class ItemsPayload(BaseModel):
    itemType: str
    itemListType: str
    itemList: list
    itemBigListURL: Optional[str]
    refId: int

    @root_validator()
    def validate_items_payload(cls,values):
        itemListType = values.get("itemListType")
        itemListTypeSet = {"list", "biglist"}
        itemList = values.get("itemList")
        itemBigListURL = values.get("itemBigListURL")

        if itemListType not in itemListTypeSet:
            raise ValueError(f"Item list type must be in {itemListTypeSet}, got '{itemListType}'")
        elif itemListType == "list" and not itemList:
            raise ValueError("Item list is empty")
        elif itemListType == "biglist" and not itemBigListURL:
            raise ValueError("Item biglist url is empty")
        return values

class ItemDetails(BaseModel):
    itemListCount: int
    itemsPayload: Optional[List[ItemsPayload]]

class MembersPayload(BaseModel):
    memberType: str
    memberList: list
    memberBigListURL: str

    @root_validator()
    def validate_members_payload(cls,values):
        memberType = values.get("memberType")
        memberTypeSet = {"list", "biglist"}
        memberList = values.get("memberList")
        memberBigListURL = values.get("memberBigListURL")

        if memberType not in memberTypeSet:
            raise ValueError(f"Member type must be in {memberTypeSet}, got '{memberType}'")
        elif memberType == "list" and not memberList:
            raise ValueError("Member list is empty")
        elif memberType == "biglist" and not memberBigListURL:
            raise ValueError("Member biglist url is empty")
        return values

class Member(BaseModel):
    memberListCount: int
    membersPayload: Optional[List[MembersPayload]]

class Award(BaseModel):
    awardType: str = Field(..., min_length=1)
    value: str = Field(..., min_length=1)
    discountMethod: str = Field(..., min_length=1)
    itemRefId: Optional[List[str]]

    @root_validator()
    def validate_award(cls,values):
        awardType = values.get("awardType")
        discountMethodSet = {constants.DOLLAR_OFF, constants.PERCENTAGE_OFF}

        if awardType == constants.ITEM_DISCOUNT:
            discountMethod = values.get("discountMethod")
            if discountMethod not in discountMethodSet:
                raise ValueError(f"Discount type must be in {discountMethodSet}, got '{discountMethod}'")
        return values

class Payload(BaseModel):
    eventTimeStamp: str = Field(..., min_length=1)
    status: str = Field(..., min_length=1)
    promotionNumber: str = Field(..., min_length=1)
    startDate: str = Field(..., min_length=1)
    endDate: str = Field(..., min_length=1)
    startTime: str = Field(..., min_length=1)
    endTime: str = Field(..., min_length=1)
    timeZone: str = Field(..., min_length=1)
    channels: List[str]
    labels: List[str]
    awardList: Optional[List[Award]] = None
    memberDetails: Optional[Member] = None
    itemDetails: Optional[ItemDetails] = None
    clubDetails: Optional[ClubDetails] = None
    membershipType: List[str]

    @root_validator()
    def validate_payload(cls,values):
        status = values.get("status")
        statusSet = {constants.PUBLISHED, constants.DISABLED, constants.LIVE}

        if status not in statusSet:
            raise ValueError(f"Status must be in {statusSet}, got '{status}'")

        labels = values.get("labels")
        if constants.TETRIS in labels and constants.TIO in labels:
            raise ValueError(f"Offer cannot be both tetris and tio")
        return values

class OfferEvent(BaseModel):
    payload : Payload

#Model to validate update member offer event
class UpdateOfferMembersEvent(BaseModel):
    offerIdList: List[int]
    memberBigListLocation: str

def process_event_data(received_event, connection_pool):
    # construct values to insert
    print("Constructing data to insert into audit table..")

    offer_uid = ''
    offer_source = ''
    try:
        offer_id_list = []
        offer_id = ''
        if 'offerIdList' in received_event:
            offer_id_list = received_event["offerIdList"]
            if offer_id_list:
                offer_id = offer_id_list[0]
        else:
            offer_id = received_event['payload']['promotionNumber']
            if "tetris" in received_event["payload"]["labels"]:
                offer_source = "tetris"
            elif "tio" in received_event["payload"]["labels"]:
                offer_source = "tio"
            else:
                offer_source = "broadreach"
        dt = datetime.now(timezone.utc)
        date_string = dt.strftime('%Y-%m-%d_%H:%M:%S.%f')
        offer_uid = str(offer_id)+"_"+date_string
        json_obj = json.dumps(received_event) #convert dict to json string

        # insert event into audit table
        print("Dumping the event into audit table")
        audit_offer_event(ouid=offer_uid, oid=offer_id, event_json=json_obj, status=constants.RECEIVED, comments='', received_date=dt, postgres_sql_pool=connection_pool)

        unconsumable_labels = [constants.JOIN, constants.PAID_TRIAL, constants.FREE_TRIAL, constants.VOUCHER, constants.SPONSORED, constants.JOIN_NO_ADDONS, constants.JOIN_NO_UPGRADE, constants.RENEW, constants.UPGRADE, constants.NILPICK, constants.FREEOSK, constants.CREDIT, constants.SIF, constants.RAF, constants.PURPOSE_MEMBERSHIP, constants.JOIN_REJOINERS_OK, constants.JOIN_NO_BUSINESS, constants.MILITARY, constants.NURSE, constants.FIRST_RESPONDER, constants.TEACHER, constants.GOVERNMENT, constants.MEDICAL_PROVIDER, constants.STUDENT, constants.GOVASSIST, constants.SENIOR, constants.HOSPITAL, constants.ALUM, constants.ACQ_AFFILIATE, constants.ACQ_AFFINITY_AUDIENCES, constants.ACQ_BIG_PROMOTION, constants.ACQ_GENERAL_ACQUISITION, constants.ACQ_PAID_SEARCH_DISPLAY, constants.ACQ_PARTNERSHIP, constants.ACQ_SOCIAL_NON_DISCOUNTED, constants.ACQ_EBG, constants.ACQ_GROUPON, constants.ACQ_VALPACK, constants.ACQ_SIF]

        # Check memberDetails
        if 'offerIdList' not in received_event and offer_source != "broadreach" and 'memberDetails' not in received_event['payload']:
            print(print_bars)
            print( f"Personalized offer missing memberDetails")
            update_offer_event_status(offer_uid, constants.SKIPPED, "Personalized offer missing memberDetails", connection_pool)
            print(print_bars)
        elif 'offerIdList' not in received_event and 'memberDetails' in received_event['payload'] and received_event['payload']['memberDetails']['memberListCount'] != 1:
            print(print_bars)
            print( f"Offers with memberListCount != 1 won't be processed.")
            update_offer_event_status(offer_uid, constants.SKIPPED, "Received an offer with memberListCount not equal to 1, skipping this offer", connection_pool)
            print(print_bars)
        # Check labels
        elif 'offerIdList' not in received_event and any(label in unconsumable_labels for label in received_event['payload']['labels']):
            print(print_bars)
            print( f"Offer won't be processed because it contains unconsumable label.")
            update_offer_event_status(offer_uid, constants.SKIPPED, "Received an offer with at least one unwanted label, skipping this offer", connection_pool)
            print(print_bars)
        else:
            # Trim awardList based on awardType
            if 'offerIdList' not in received_event:
                for award in received_event['payload']['awardList']:
                    if award['awardType'] != "DISCOUNT_ITEM_PRICE":
                        received_event['payload']['awardList'].remove(award)
                print( f"received_event after trimming:                        { received_event }")
                if not received_event['payload']['awardList']:
                    print(print_bars)
                    print( f"Offer does not contain award of type DISCOUNT_ITEM_PRICE.")
                    update_offer_event_status(offer_uid, constants.SKIPPED, "Received an offer without award of type DISCOUNT_ITEM_PRICE, skipping this offer", connection_pool)
                    print(print_bars)
                    return

            try:
                print("Validate event")
                if 'offerIdList' in received_event:
                    model = UpdateOfferMembersEvent(**received_event)
                else:
                    model = OfferEvent(**received_event)
                print(model.dict())
            except ValidationError as e:
                print("Incorrect attributes in the event")
                print(e)
                update_offer_event_status(offer_uid, constants.FAILED, str(e), connection_pool)
                send_failure_mail(offer_id, offer_source, e, "PayloadError")
                send_teams_failure_message(offer_id, offer_source, e, "PayloadError")
            else:
                print("Ready to process the event")
                update_offer_event_status(offer_uid, constants.PROCESSING, "", connection_pool)
                if 'offerIdList' in received_event:
                    process_event = process_update_memberlist_event(received_event, connection_pool)
                else:
                    process_event = navigate_event(received_event, connection_pool)
                update_offer_event_status(offer_uid, constants.PROCESSED, process_event, connection_pool)

    except (DataframeTransformException) as error:
        print("Error occured while transforming event to Dataframe: ", error)
        update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
        send_failure_mail(offer_id, offer_source, error, "PayloadError")
        send_teams_failure_message(offer_id, offer_source, error, "PayloadError")

    except (psycopg2.DatabaseError) as error:
        print("Error occured while inserting event to PostgreSQL: ", error)
        update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
        send_failure_mail(offer_id, offer_source, error, "DatabaseError")
        sys.exit(str(error))

    except (BigQueryException) as error:
        print("Error occured while firing a Big query: ", error)
        update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
        send_failure_mail(offer_id, offer_source, error, "BigQueryError")
        sys.exit(str(error))

    except (BlobReadException) as error:
        print("Error occured while reading the biglist: ", error)
        update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
        send_failure_mail(offer_id, offer_source, error, "BlobReadError")
        send_teams_failure_message(offer_id, offer_source, error, "BlobReadError")

    except (CacheInsertionFailed, DbConnectionFailed, SecretFetchFailed, DbInsertionFailed) as error:
        print("System error occured while processing event: ", error)
        update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
        send_failure_mail(offer_id, offer_source, error, "SystemError")
        sys.exit(str(error))

    except (Exception) as error:
        print("Error occured while processing event: ", error)
        update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
        send_failure_mail(offer_id, offer_source, error, "Error")

def preprocess_event(event):
    try:
        event_string = event.decode("utf-8")
        event_string = event_string.replace("'", "''")
        print( f"event_string:                        { event_string }")
        received_event = json.loads(event_string)
        print( f"received_event:                        { received_event }")
    except (Exception) as error:
        print("Error occured while processing event string:", error)
        send_failure_mail('[INVALID_PAYLOAD_FOUND]', 'Unknown', event, "PayloadError")
        send_teams_failure_message('[INVALID_PAYLOAD_FOUND]', 'Unknown', event, "PayloadError")
        return None

    connection_pool = None
    try:
        secrets = fetch_postgres_secrets()
        connection_pool = create_connection_pool(secrets)
        print(print_bars)
        print("----------------------------------------- STARTING PROCESSING EVENT -----------------------------------------")
        print(print_bars)

        process_event_data(received_event, connection_pool)

    finally:
        close_connection_pool(connection_pool)

def set_event_horizon_env():
    if os.environ.get('ENV') == constants.DEV:
        return Environment.DEV
    elif os.environ.get('ENV') == constants.STAGE:
        return Environment.STAGE
    elif os.environ.get('ENV') == constants.PROD:
        return Environment.PROD

def set_channel_alias():
    if os.environ.get('ENV') == constants.DEV:
        return constants.OB_DEV_CHANNEL_ALIAS
    elif os.environ.get('ENV') == constants.STAGE or os.environ.get('ENV') == constants.PROD:
        return constants.QUEST_CHANNEL_ALIAS

async def initialize_event_horizon():
    event_horizon_product_id, event_horizon_token = fetch_event_horizon_secrets()

    options: ClientOptions = ClientOptions(
        product_id = event_horizon_product_id,
        credentials = Credentials(
            token       = event_horizon_token,
            environment = set_event_horizon_env()
        )
    )

    async with Client( options ) as client:
        subscribe_options: SubscribeSyncOptions = SubscribeSyncOptions(
            channel_alias = set_channel_alias(),
            arguments     = {},
            timeout       = timedelta( seconds = 30 )
        )

        while True:
            try:
                packet: EventPacket = await client.subscribe_sync( subscribe_options )

                event: Event = packet.event
                if event.body is None:
                    break
                preprocess_event(event.body)

                await packet.acker.ack()

            except TimeoutException:
                break

if __name__ == '__main__':
    run( initialize_event_horizon() )

#spark-submit --master local --queue default --packages org.postgresql:postgresql:42.3.6,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2 --verbose /offer-data-loader/data_loader/sams_offer_bank/offer_bank/EventHubReader.py
