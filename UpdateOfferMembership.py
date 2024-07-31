import offer_bank.database.query_db_v2 as query_db
from offer_bank.database.db_operations import read_database, insert_into_database
from offer_bank.blob_storage.blob_operations import read_from_blob_v2
import offer_bank.cache.member_cache as member_cache 
from offer_bank.utility.common_code_v2 import list_to_string, get_membership_uuid_from_membership_id
from offer_bank.utility.map_creation import create_member_offers_cache_map
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from offer_bank.spark_config.spark import get_spark
from offer_bank.exceptions.cache_insertion_failed import CacheInsertionFailed
from offer_bank.exceptions.offer_not_exist_exception import OfferNotExistException
from offer_bank.exceptions.db_insertion_failed import DbInsertionFailed
from offer_bank.exceptions.blob_read_exception import BlobReadException
from offer_bank.exceptions.big_query_exception import BigQueryException
import offer_bank.bigquery.bigquery_operations as bq_operations
from offer_bank.utility import constants


spark = get_spark()

def fetch_offer_details(offer_id_list_string, connection_pool):
    """Fetch the offer details from the db"""
    print(f'Fetching offer list details from the database :')
    offer_details = query_db.query_non_br_offer_details(offer_id_list_string, connection_pool)
    print(f"Non BR Offer list details : {offer_details}")
    if (offer_details == None or offer_details == "" or offer_details == []):
        raise OfferNotExistException("The offers to update do not exist")
    return offer_details

def define_schema(offer_source, new_members_applicable_for_offer):
    if(offer_source.casefold() == 'Tetris'.casefold()):
        new_members_applicable_for_offer = new_members_applicable_for_offer.withColumnRenamed('MembershipNumber','membership_id')\
                     .withColumnRenamed('Propensity','propensity')
    else:
        new_members_applicable_for_offer = new_members_applicable_for_offer.toDF('MembershipNumber')
        new_members_applicable_for_offer = new_members_applicable_for_offer.withColumnRenamed('MembershipNumber','membership_id')
    return new_members_applicable_for_offer

def display_members_to_add(members_to_add_list):
    if len(members_to_add_list) < 1:
                print("No new members to add in the received member list")
    else:
        print(f'Members to add : {members_to_add_list}')
        print(f"Number of memberships to add : {len(members_to_add_list)}")

def display_members_to_delete(members_to_delete_list):
    if len(members_to_delete_list) < 1:
                print("No exisiting members to remove in the received member list")
    else:
        print(f'Members to delete : {members_to_delete_list}')
        print(f"Number of memberships to delete : {len(members_to_delete_list)}")

def delete_cancelled_members_from_db_and_bigquery(members_to_delete_list, offer_id, connection_pool):
    if(len(members_to_delete_list)!= 0):
        print(f"Deleting members from member_offers table in database and BigQuery associated with offer_id {offer_id} :")
        members_to_delete_string = list_to_string(members_to_delete_list)
        query_db.delete_members(offer_id, members_to_delete_string, connection_pool)
        bq_operations.delete_expired_members(offer_id, members_to_delete_string)

def process_update_memberlist_event(event, connection_pool):
    offer_id_list = event["offerIdList"]
   
    offer_id_list_string = list_to_string(offer_id_list)
    offer_details= fetch_offer_details(offer_id_list_string, connection_pool)

    """Read the new memberlist from blob"""
    offer_source = offer_details[0][1]
    print(f"Membership list updated for {offer_source} offers")

    event_member_big_list_location = event['memberBigListLocation']
    try:
        new_memberslist = read_from_blob_v2(event_member_big_list_location, offer_source, constants.MEMBER_BLOB)
    except Exception as e:
            raise BlobReadException(constants.MEMBER_BLOB, event_member_big_list_location, e)
    new_members_applicable_for_offer = define_schema(offer_source, new_memberslist)
    
    print("Fetching membership_uuid for the corresponding membership_number for the new membership list :")   
    new_members_uuid_applicable_for_offer = get_membership_uuid_from_membership_id(new_members_applicable_for_offer, offer_source)

    """Load member_offers table in a dataframe"""
    member_offers_table = read_database(table_name = "member_offers")
    
    members_to_update_in_cache = set()
    
    processed_offer_ids = []
    """Processing each offer"""
    for offer in offer_details:
        
        offer_id = offer[0]
        member_list_location = offer[2]
        

        if(member_list_location != event_member_big_list_location):
            
            print(f"--------------------Processing OfferId. : {offer_id} --------------------")
            
            previous_members_applicable_for_offer = member_offers_table.filter(col("offer_id") == offer_id)
            
            members_to_delete = previous_members_applicable_for_offer.select('membership_uuid')\
                                .subtract(new_members_uuid_applicable_for_offer.select('membership_uuid'))
            
            members_to_add =  new_members_uuid_applicable_for_offer.select('membership_uuid')\
                                .subtract(previous_members_applicable_for_offer.select('membership_uuid'))
            

            """Preparing members to be added to postgres payload"""
            members_to_add_df = new_members_uuid_applicable_for_offer. \
                                join(members_to_add, members_to_add.membership_uuid == new_members_uuid_applicable_for_offer.membership_uuid, "leftsemi")
            members_to_add_df = members_to_add_df.withColumn("offer_id", lit(offer_id))
            

            
            """Adding members to be added and removed to a list"""
            members_to_add_list = members_to_add_df.select('membership_uuid').rdd.flatMap(lambda x: x).collect()
            display_members_to_add(members_to_add_list)
            

            members_to_delete_list = members_to_delete.select('membership_uuid').rdd.flatMap(lambda x: x).collect()
            display_members_to_delete(members_to_delete_list)

            members_to_update_in_cache.update(members_to_add_list)
            members_to_update_in_cache.update(members_to_delete_list)
            
            
           
            """Delete the members from member_offers table in DB and BigQuery"""
            delete_cancelled_members_from_db_and_bigquery(members_to_delete_list, offer_id, connection_pool)
            
            """Update the offer in offers table"""
            print(f"Updating offer having offer id {offer_id} in offers table:")
            query_db.update_offer(offer_id, event_member_big_list_location, connection_pool)

            """Update the offer in bigquery table"""
            print(f"Updating offer having offer id {offer_id} in offers table:")
            bq_operations.update_member_biglist_in_offer(offer_id, event_member_big_list_location)
            
            if members_to_add_df.count() > 0:
                members_to_add_df = members_to_add_df.withColumn("membership_id",col("membership_id").cast(StringType()))
                """Insert the members into member_offers table"""
                print(f"Inserting members into member_offers table in database associated with offer_id {offer_id} :")
                try:
                    insert_into_database(table_name = "member_offers", df = members_to_add_df)
                except (Exception) as error:
                    print(f"Error inserting data to database: {error}")
                    raise DbInsertionFailed(error)

                print(f"Inserting members into member_offers table in BigQuery associated with offer_id {offer_id} :")
                try:
                    bq_operations.insert_into_bigquery(table_name = "member_offers", df = members_to_add_df)
                except (Exception) as error:
                    print(f"Error inserting data to BigQuery: {error}")
                    raise BigQueryException(error)

        """Tracking processed offer_ids"""
        processed_offer_ids.append(offer_id)
        
    """Refreshing the cache"""
    print(f"Members to update in cache = {len(members_to_update_in_cache)}")
    if(len(members_to_update_in_cache)!= 0):
        members_to_update_in_cache = list(members_to_update_in_cache)

        members_to_update_in_cache_df = spark.createDataFrame(members_to_update_in_cache, StringType()).withColumnRenamed("value", "membership_uuid")
        member_offers_cache_map = create_member_offers_cache_map(members_to_update_in_cache_df)

        """Filtering membership having no offer after the update in membership list"""
        members_to_delete = [i for i in members_to_update_in_cache if i not in member_offers_cache_map]
        member_cache.delete_from_cache(members_to_delete)

        try:
            member_cache.insert_into_cache(member_offers_cache_map)
        except (Exception) as error:
            raise CacheInsertionFailed(constants.MEMBER_CACHE) 
            

    """Collecting the list of offer_ids not processed as they were not present in the database"""        
    unprocessed_offer_ids = [x for x in offer_id_list if x not in processed_offer_ids]
    
    return f"Memeberlist updated successfully of offer_ids : {processed_offer_ids}. Ignored processing {unprocessed_offer_ids}."
