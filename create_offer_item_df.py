def create_offer_item_df(event_df):
    """Create DataFrames for items and offer items based on the event data."""
    item_schema = StructType() \
        .add("item_number", LongType(), True) \
        .add("item_type", StringType(), True)

    items_df = spark.createDataFrame([], schema=item_schema)

    # Extract the offer ID from the event DataFrame
    offer_id = event_df.first()['payload']['promotionNumber']

    # Process each item in the event's item details
    items_payload = event_df.first().get('payload', {}).get('itemDetails', {}).get('itemsPayload', [])
    for item in items_payload:
        if not should_process_item(event_df, item):
            continue

        if item['itemListType'].lower() == "list":
            items_df = process_list_type_items(item, items_df)
        else:
            items_df = process_biglist_type_items(item, event_df, items_df)

    items_df = filter_tire_items(items_df)
    offer_item_df = add_offer_id_and_product_id(items_df, offer_id)

    return items_df, offer_item_df


def should_process_item(event_df, item):
    """Determine whether the current item should be processed."""
    return item['refId'] in event_df.first()['payload']['awardList'][0]['itemRefId']


def process_list_type_items(item, items_df):
    """Process items of type 'list' and add them to the DataFrame."""
    item_list = []
    for item_number in item['itemList']:
        item_type = determine_item_type(item)
        item_list.append((item_number, item_type))
    new_items = spark.createDataFrame(item_list, ["item_number", "item_type"])
    return items_df.union(new_items)


def process_biglist_type_items(item, event_df, items_df):
    """Process items of type 'biglist' from a blob storage and add them to the DataFrame."""
    item_list_url = item['itemBigListURL']
    offer_source = event_df.first()['payload']['offerSource']
    try:
        new_items = read_from_blob_v2(item_list_url, offer_source, constants.ITEM_BLOB)
        new_items = new_items.drop("Propensity")
        new_items = rename_item_columns(new_items, offer_source)
        item_type = determine_item_type(item)
        new_items = new_items.withColumn("item_type", lit(item_type))
        return items_df.union(new_items)
    except Exception as e:
        raise BlobReadException(constants.ITEM_BLOB, item_list_url, e)


def determine_item_type(item):
    """Determine the item type based on the item type constant."""
    if item['itemType'] == constants.DISCOUNTED_ITEMS:
        return constants.DISCOUNTED_ITEM
    elif item['itemType'] == constants.ELIGIBLE_ITEMS:
        return constants.ELIGIBLE_ITEM
    return None


def rename_item_columns(new_items, offer_source):
    """Rename the item columns based on the offer source."""
    if offer_source.lower() in {'broadreach', 'tio'}:
        new_items = new_items.toDF("ItemNumber")
    return new_items.withColumnRenamed("ItemNumber", "item_number")


def add_offer_id_and_product_id(items_df, offer_id):
    """Add offer ID and product ID to the items DataFrame."""
    items_df = items_df.withColumn("item_number", col("item_number").cast(LongType()))
    offer_item_df = items_df.withColumn("offer_id", lit(offer_id).cast(IntegerType()))
    return offer_item_df.withColumn("product_id", when(lit(True), lit("")))
