def process_update_memberlist_event(event, connection_pool):
    offer_id_list = event["offerIdList"]
    offer_id_list_string = list_to_string(offer_id_list)
    offer_details = fetch_offer_details(offer_id_list_string, connection_pool)

    """Read the new memberlist from blob"""
    offer_source = offer_details[0][1]
    print(f"Membership list updated for {offer_source} offers")

    event_member_big_list_location = event['memberBigListLocation']
    new_members_applicable_for_offer = read_new_members_list(event_member_big_list_location, offer_source)

    new_members_uuid_applicable_for_offer = get_membership_uuid_from_membership_id(new_members_applicable_for_offer, offer_source)

    """Load member_offers table in a dataframe"""
    member_offers_table = read_database(table_name="member_offers")

    members_to_update_in_cache = set()
    processed_offer_ids = []

    for offer in offer_details:
        offer_id = offer[0]
        member_list_location = offer[2]

        if member_list_location != event_member_big_list_location:
            process_offer_update(
                offer_id, member_list_location, new_members_uuid_applicable_for_offer, 
                member_offers_table, members_to_update_in_cache, event_member_big_list_location, connection_pool
            )
            processed_offer_ids.append(offer_id)

    refresh_cache(members_to_update_in_cache)

    unprocessed_offer_ids = [x for x in offer_id_list if x not in processed_offer_ids]

    return f"Memberlist updated successfully for offer_ids: {processed_offer_ids}. Ignored processing: {unprocessed_offer_ids}."


def read_new_members_list(event_member_big_list_location, offer_source):
    """Read new members list from blob storage and define schema."""
    try:
        new_memberslist = read_from_blob_v2(event_member_big_list_location, offer_source, constants.MEMBER_BLOB)
        return define_schema(offer_source, new_memberslist)
    except Exception as e:
        raise BlobReadException(constants.MEMBER_BLOB, event_member_big_list_location, e)


def process_offer_update(
    offer_id, member_list_location, new_members_uuid_applicable_for_offer, 
    member_offers_table, members_to_update_in_cache, event_member_big_list_location, connection_pool
):
    print(f"--------------------Processing OfferId: {offer_id} --------------------")

    previous_members_applicable_for_offer = member_offers_table.filter(col("offer_id") == offer_id)

    members_to_delete = get_members_to_delete(previous_members_applicable_for_offer, new_members_uuid_applicable_for_offer)
    members_to_add_df = get_members_to_add(new_members_uuid_applicable_for_offer, previous_members_applicable_for_offer, offer_id)

    members_to_add_list = members_to_add_df.select('membership_uuid').rdd.flatMap(lambda x: x).collect()
    display_members_to_add(members_to_add_list)

    members_to_delete_list = members_to_delete.select('membership_uuid').rdd.flatMap(lambda x: x).collect()
    display_members_to_delete(members_to_delete_list)

    members_to_update_in_cache.update(members_to_add_list)
    members_to_update_in_cache.update(members_to_delete_list)

    delete_cancelled_members_from_db_and_bigquery(members_to_delete_list, offer_id, connection_pool)

    update_offer_details_in_db_and_bigquery(offer_id, event_member_big_list_location)

    if members_to_add_df.count() > 0:
        insert_members_to_db_and_bigquery(members_to_add_df, offer_id)


def get_members_to_delete(previous_members, new_members):
    """Calculate members to be deleted."""
    return previous_members.select('membership_uuid').subtract(new_members.select('membership_uuid'))


def get_members_to_add(new_members, previous_members, offer_id):
    """Calculate and prepare members to be added."""
    members_to_add = new_members.select('membership_uuid').subtract(previous_members.select('membership_uuid'))
    members_to_add_df = new_members.join(members_to_add, members_to_add.membership_uuid == new_members.membership_uuid, "leftsemi")
    return members_to_add_df.withColumn("offer_id", lit(offer_id))


def update_offer_details_in_db_and_bigquery(offer_id, member_big_list_location):
    """Update offer details in database and BigQuery."""
    print(f"Updating offer with offer_id {offer_id} in offers table:")
    query_db.update_offer(offer_id, member_big_list_location, connection_pool)
    print(f"Updating offer with offer_id {offer_id} in BigQuery:")
    bq_operations.update_member_biglist_in_offer(offer_id, member_big_list_location)


def insert_members_to_db_and_bigquery(members_to_add_df, offer_id):
    """Insert members into the database and BigQuery."""
    print(f"Inserting members into member_offers table in database associated with offer_id {offer_id}:")
    try:
        insert_into_database(table_name="member_offers", df=members_to_add_df)
    except Exception as error:
        print(f"Error inserting data to database: {error}")
        raise DbInsertionFailed(error)

    print(f"Inserting members into member_offers table in BigQuery associated with offer_id {offer_id}:")
    try:
        bq_operations.insert_into_bigquery(table_name="member_offers", df=members_to_add_df)
    except Exception as error:
        print(f"Error inserting data to BigQuery: {error}")
        raise BigQueryException(error)


def refresh_cache(members_to_update_in_cache):
    """Refresh cache with updated members."""
    print(f"Members to update in cache = {len(members_to_update_in_cache)}")
    if members_to_update_in_cache:
        members_to_update_in_cache_df = spark.createDataFrame(list(members_to_update_in_cache), StringType()).withColumnRenamed("value", "membership_uuid")
        member_offers_cache_map = create_member_offers_cache_map(members_to_update_in_cache_df)

        """Filter membership having no offer after the update in membership list"""
        members_to_delete = [i for i in members_to_update_in_cache if i not in member_offers_cache_map]
        member_cache.delete_from_cache(members_to_delete)

        try:
            member_cache.insert_into_cache(member_offers_cache_map)
        except Exception as error:
            raise CacheInsertionFailed(constants.MEMBER_CACHE)
