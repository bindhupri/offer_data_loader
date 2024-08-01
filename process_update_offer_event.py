def process_updated_offer_event(connection_pool, event, db_offer):
    offer_id = event["payload"]["promotionNumber"]
    offer_source = determine_offer_source(event)
    event["payload"]["offerSource"] = offer_source

    if db_offer[0][1] != offer_source:
        return invalid_update_payload_message(db_offer, offer_id)

    preprocess_event_dates(event)

    """Null check for timezone and setting default value to CST"""
    timezone = event["payload"]["timeZone"]
    if not timezone:
        event["payload"]["timeZone"] = "CST"

    """Resolve club list data"""
    event = common_code.resolve_club_list(event, offer_source)

    """Process the incoming event as map to dataframe"""
    event_df = create_event_dataframe(event)

    """Generate offer and items dataframe to update into the db"""
    offers_df, items_df, offer_item_df = generate_dataframes(event_df)

    club_override_df = create_club_override_dataframe(event_df)

    update_offer_items(connection_pool, offer_id, items_df, offer_item_df)

    update_club_overrides(connection_pool, offer_id, club_override_df)

    update_offers(connection_pool, offers_df, offer_id)

    update_offer_cache(event_df, club_override_df, offer_id, items_df)

    return f"Offer {offer_id} updated successfully."


def determine_offer_source(event):
    """Determine the source of the offer from the event labels."""
    labels = event["payload"]["labels"]
    if "tetris" in labels:
        return "TETRIS"
    elif "tio" in labels:
        return "TIO"
    return "BROADREACH"


def invalid_update_payload_message(db_offer, offer_id):
    """Return a message for an invalid update payload."""
    print(f"Invalid update payload.")
    return f"Invalid update payload. Offer source {db_offer[0][1]} of offer {offer_id} cannot be updated once created."


def preprocess_event_dates(event):
    """Preprocess date and time before creating a dataframe."""
    event["payload"]["startDate"] += " " + event["payload"]["startTime"]
    event["payload"]["endDate"] += " " + event["payload"]["endTime"]
    event["payload"]["eventTimeStamp"] = event["payload"]["eventTimeStamp"][:19].replace("T", " ")


def create_event_dataframe(event):
    """Create a DataFrame from the event."""
    try:
        event_df = common_code.process_event(event)
        print("-------------------------------------------Event DataFrame-------------------------------------------")
        event_df.printSchema()
        return event_df
    except Exception as error:
        raise DataframeTransformException(error)


def generate_dataframes(event_df):
    """Generate DataFrames for offers and items from the event DataFrame."""
    print("Generate offer and items dataframe to update into the db")
    offers_df = common_code.create_offer_df(event_df)
    print("-------------------------------------------offers_df DataFrame-------------------------------------------")
    offers_df.printSchema()

    try:
        items_df, offer_item_df = common_code.create_offer_item_df(event_df)
        print("-------------------------------------------items_df DataFrame-------------------------------------------")
        items_df.printSchema()
        print("-------------------------------------------offer_item_df DataFrame-------------------------------------------")
        offer_item_df.printSchema()
        return offers_df, items_df, offer_item_df
    except BlobReadException as e:
        raise BlobReadException(constants.ITEM_BLOB, event_df.first()['offerAttributes_itemBigList'], e)


def create_club_override_dataframe(event_df):
    """Create a DataFrame for club overrides."""
    club_override_df = common_code.create_club_override_df(event_df)
    print("-------------------------------------------club_override_df DataFrame-------------------------------------------")
    club_override_df.printSchema()
    return club_override_df


def update_offer_items(connection_pool, offer_id, items_df, offer_item_df):
    """Update offer items in the database and BigQuery."""
    print("deleting old and inserting new items")
    delete_items(offer_id, connection_pool)

    bq_operations.bigquery_delete_offer_items(offer_id)

    try:
        insert_into_database('offer_items_v2', offer_item_df)
        bq_operations.insert_into_bigquery('offer_items', offer_item_df)
    except Exception as error:
        raise DbInsertionFailed(f"Error inserting data to database: {error}")


def update_club_overrides(connection_pool, offer_id, club_override_df):
    """Update club overrides in the database and BigQuery."""
    print("deleting old and inserting new club overrides")
    delete_club_overrides(offer_id, connection_pool)

    bq_operations.bigquery_delete_club_overrides(offer_id)

    if not club_override_df.rdd.isEmpty():
        try:
            insert_into_database('club_overrides', club_override_df)
            bq_operations.insert_into_bigquery('club_overrides', club_override_df)
        except Exception as error:
            raise DbInsertionFailed(f"Error inserting data to database: {error}")


def update_offers(connection_pool, offers_df, offer_id):
    """Update offers in the database and BigQuery."""
    print("Updating offer in offers table in the database")
    offers_dict = offers_df.toPandas().to_dict('list')
    update_offer_details(offers_dict, connection_pool)

    try:
        bq_operations.insert_into_bigquery('offers', offers_df)
    except Exception as error:
        raise BigQueryException(f"Error inserting data to BigQuery: {error}")


def update_offer_cache(event_df, club_override_df, offer_id, items_df):
    """Update the offer cache."""
    items_list = items_df.select('item_number').where(items_df.item_type == constants.DISCOUNTED_ITEM).rdd.flatMap(lambda x: x).collect()
    eligible_items_list = items_df.select('item_number').where(items_df.item_type == constants.ELIGIBLE_ITEM).rdd.flatMap(lambda x: x).collect()
    print("Updating offer details in offer cache:")
    offer_cache_map = map_creation.create_offer_cache_map_v2(event_df, club_override_df, offer_id, items_list, eligible_items_list)
    try:
        print('Inserting offer in cache:')
        offer_cache.insert_into_cache(offer_cache_map)
    except Exception as error:
        raise CacheInsertionFailed(constants.OFFER_CACHE)
