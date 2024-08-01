def process_event_data(received_event, connection_pool):
    print("Constructing data to insert into audit table..")

    offer_uid = ''
    offer_source = ''
    try:
        offer_id = extract_offer_id(received_event)
        offer_source = determine_offer_source(received_event)
        dt, date_string = get_current_timestamp()
        offer_uid = f"{offer_id}_{date_string}"
        json_obj = json.dumps(received_event)  # convert dict to json string

        # insert event into audit table
        print("Dumping the event into audit table")
        audit_offer_event(offer_uid, offer_id, json_obj, constants.RECEIVED, '', dt, connection_pool)

        # Validate member details and labels
        if not is_valid_member_details(received_event, offer_source):
            update_and_log_skipped(offer_uid, "Personalized offer missing memberDetails or memberListCount != 1", connection_pool)
            return
        if contains_unconsumable_labels(received_event):
            update_and_log_skipped(offer_uid, "Offer contains unconsumable label", connection_pool)
            return

        # Trim awardList based on awardType
        if not trim_award_list(received_event):
            update_and_log_skipped(offer_uid, "Offer does not contain award of type DISCOUNT_ITEM_PRICE", connection_pool)
            return

        # Validate event payload
        if not validate_event(received_event, offer_uid, connection_pool, offer_id, offer_source):
            return

        print("Ready to process the event")
        update_offer_event_status(offer_uid, constants.PROCESSING, "", connection_pool)
        process_and_update_event(received_event, connection_pool, offer_uid)
        
    except (DataframeTransformException, psycopg2.DatabaseError, BigQueryException, BlobReadException) as error:
        handle_known_exceptions(error, offer_uid, connection_pool, offer_id, offer_source)
    except (CacheInsertionFailed, DbConnectionFailed, SecretFetchFailed, DbInsertionFailed) as error:
        handle_system_errors(error, offer_uid, connection_pool, offer_id, offer_source)
    except Exception as error:
        handle_general_error(error, offer_uid, connection_pool, offer_id, offer_source)

def extract_offer_id(received_event):
    if 'offerIdList' in received_event:
        offer_id_list = received_event["offerIdList"]
        return offer_id_list[0] if offer_id_list else ''
    else:
        return received_event['payload']['promotionNumber']

def determine_offer_source(received_event):
    if 'offerIdList' not in received_event:
        labels = received_event['payload']['labels']
        if "tetris" in labels:
            return "tetris"
        elif "tio" in labels:
            return "tio"
    return "broadreach"

def get_current_timestamp():
    dt = datetime.now(timezone.utc)
    date_string = dt.strftime('%Y-%m-%d_%H:%M:%S.%f')
    return dt, date_string

def is_valid_member_details(received_event, offer_source):
    if 'offerIdList' not in received_event:
        if offer_source != "broadreach" and 'memberDetails' not in received_event['payload']:
            return False
        if 'memberDetails' in received_event['payload'] and received_event['payload']['memberDetails']['memberListCount'] != 1:
            return False
    return True

def contains_unconsumable_labels(received_event):
    unconsumable_labels = [constants.JOIN, constants.PAID_TRIAL, constants.FREE_TRIAL, constants.VOUCHER, constants.SPONSORED, constants.JOIN_NO_ADDONS, constants.JOIN_NO_UPGRADE, constants.RENEW, constants.UPGRADE, constants.NILPICK, constants.FREEOSK, constants.CREDIT, constants.SIF, constants.RAF, constants.PURPOSE_MEMBERSHIP, constants.JOIN_REJOINERS_OK, constants.JOIN_NO_BUSINESS, constants.MILITARY, constants.NURSE, constants.FIRST_RESPONDER, constants.TEACHER, constants.GOVERNMENT, constants.MEDICAL_PROVIDER, constants.STUDENT, constants.GOVASSIST, constants.SENIOR, constants.HOSPITAL, constants.ALUM, constants.ACQ_AFFILIATE, constants.ACQ_AFFINITY_AUDIENCES, constants.ACQ_BIG_PROMOTION, constants.ACQ_GENERAL_ACQUISITION, constants.ACQ_PAID_SEARCH_DISPLAY, constants.ACQ_PARTNERSHIP, constants.ACQ_SOCIAL_NON_DISCOUNTED, constants.ACQ_EBG, constants.ACQ_GROUPON, constants.ACQ_VALPACK, constants.ACQ_SIF]
    return any(label in unconsumable_labels for label in received_event['payload']['labels'])

def trim_award_list(received_event):
    if 'offerIdList' not in received_event:
        award_list = received_event['payload'].get('awardList', [])
        received_event['payload']['awardList'] = [award for award in award_list if award['awardType'] == "DISCOUNT_ITEM_PRICE"]
        return bool(received_event['payload']['awardList'])
    return True

def validate_event(received_event, offer_uid, connection_pool, offer_id, offer_source):
    try:
        if 'offerIdList' in received_event:
            UpdateOfferMembersEvent(**received_event)
        else:
            OfferEvent(**received_event)
        return True
    except ValidationError as e:
        print("Incorrect attributes in the event")
        print(e)
        update_offer_event_status(offer_uid, constants.FAILED, str(e), connection_pool)
        send_failure_mail(offer_id, offer_source, e, "PayloadError")
        send_teams_failure_message(offer_id, offer_source, e, "PayloadError")
        return False

def process_and_update_event(received_event, connection_pool, offer_uid):
    if 'offerIdList' in received_event:
        process_event = process_update_memberlist_event(received_event, connection_pool)
    else:
        process_event = navigate_event(received_event, connection_pool)
    update_offer_event_status(offer_uid, constants.PROCESSED, process_event, connection_pool)

def update_and_log_skipped(offer_uid, reason, connection_pool):
    print(print_bars)
    print(reason)
    update_offer_event_status(offer_uid, constants.SKIPPED, reason, connection_pool)
    print(print_bars)

def handle_known_exceptions(error, offer_uid, connection_pool, offer_id, offer_source):
    print(f"Error occurred: {error}")
    update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
    send_failure_mail(offer_id, offer_source, error, "KnownError")
    send_teams_failure_message(offer_id, offer_source, error, "KnownError")
    if isinstance(error, (psycopg2.DatabaseError, BigQueryException)):
        sys.exit(str(error))

def handle_system_errors(error, offer_uid, connection_pool, offer_id, offer_source):
    print(f"System error occurred: {error}")
    update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
    send_failure_mail(offer_id, offer_source, error, "SystemError")
    sys.exit(str(error))

def handle_general_error(error, offer_uid, connection_pool, offer_id, offer_source):
    print(f"Error occurred while processing event: {error}")
    update_offer_event_status(offer_uid, constants.FAILED, str(error), connection_pool)
    send_failure_mail(offer_id, offer_source, error, "Error")
