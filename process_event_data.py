def handle_offer_status(message, offer_uid, status, reason, connection_pool):
    print(message)
    update_offer_event_status(offer_uid, status, reason, connection_pool)


def get_offer_details(received_event):
    offer_id = ''
    offer_source = ''

    offer_id_list = received_event.get('offerIdList', [])
    if offer_id_list:
        offer_id = offer_id_list[0]
    else:
        offer_id = received_event['payload'].get('promotionNumber', '')
        labels = received_event['payload'].get('labels', [])
        
        if "tetris" in labels:
            offer_source = "tetris"
        elif "tio" in labels:
            offer_source = "tio"
        else:
            offer_source = "broadreach"
    
    return offer_id, offer_source



def process_event_data(received_event, connection_pool):
    # construct values to insert
    print("Constructing data to insert into audit table..")

    offer_uid = ''
    offer_source = ''
    try:
        offer_id, offer_source = get_offer_details(received_event)
        dt = datetime.now(timezone.utc)
        date_string = dt.strftime('%Y-%m-%d_%H:%M:%S.%f')
        offer_uid = str(offer_id)+"_"+date_string
        json_obj = json.dumps(received_event) #convert dict to json string

        # insert event into audit table
        print("Dumping the event into audit table")
        audit_offer_event(ouid=offer_uid, oid=offer_id, event_json=json_obj, status=constants.RECEIVED, comments='', received_date=dt, postgres_sql_pool=connection_pool)

        unconsumable_labels = [constants.JOIN, constants.PAID_TRIAL, constants.FREE_TRIAL, constants.VOUCHER, constants.SPONSORED, constants.JOIN_NO_ADDONS, constants.JOIN_NO_UPGRADE, constants.RENEW, constants.UPGRADE, constants.NILPICK, constants.FREEOSK, constants.CREDIT, constants.SIF, constants.RAF, constants.PURPOSE_MEMBERSHIP, constants.JOIN_REJOINERS_OK, constants.JOIN_NO_BUSINESS, constants.MILITARY, constants.NURSE, constants.FIRST_RESPONDER, constants.TEACHER, constants.GOVERNMENT, constants.MEDICAL_PROVIDER, constants.STUDENT, constants.GOVASSIST, constants.SENIOR, constants.HOSPITAL, constants.ALUM, constants.ACQ_AFFILIATE, constants.ACQ_AFFINITY_AUDIENCES, constants.ACQ_BIG_PROMOTION, constants.ACQ_GENERAL_ACQUISITION, constants.ACQ_PAID_SEARCH_DISPLAY, constants.ACQ_PARTNERSHIP, constants.ACQ_SOCIAL_NON_DISCOUNTED, constants.ACQ_EBG, constants.ACQ_GROUPON, constants.ACQ_VALPACK, constants.ACQ_SIF]

        # Check memberDetails

        # Check memberDetails
        if 'offerIdList' not in received_event and offer_source != "broadreach" and 'memberDetails' not in received_event['payload']:
            print(print_bars)
            handle_offer_status(
                "Personalized offer missing memberDetails",
                offer_uid,
                constants.SKIPPED,
                "Personalized offer missing memberDetails",
                connection_pool
            )
            print(print_bars)

        elif 'offerIdList' not in received_event and 'memberDetails' in received_event['payload'] and received_event['payload']['memberDetails']['memberListCount'] != 1:
            print(print_bars)
            handle_offer_status(
                "Offers with memberListCount != 1 won't be processed.",
                offer_uid,
                constants.SKIPPED,
                "Received an offer with memberListCount not equal to 1, skipping this offer",
                connection_pool
            )
            print(print_bars)
        # Check labels
        elif 'offerIdList' not in received_event and any(label in unconsumable_labels for label in received_event['payload']['labels']):
            print(print_bars)
            handle_offer_status(
                "Offer won't be processed because it contains unconsumable label.",
                offer_uid,
                constants.SKIPPED,
                "Received an offer with at least one unwanted label, skipping this offer",
                connection_pool
            )
            print(print_bars)
        else:
            # Trim awardList based on awardType
            if 'offerIdList' not in received_event:
                for award in received_event['payload']['awardList']:
                    if award['awardType'] != "DISCOUNT_ITEM_PRICE":
                        received_event['payload']['awardList'].remove(award)
                print(f"received_event after trimming: {received_event}")
                if not received_event['payload']['awardList']:
                    print(print_bars)
                    handle_offer_status(
                        "Offer does not contain award of type DISCOUNT_ITEM_PRICE.",
                        offer_uid,
                        constants.SKIPPED,
                        "Received an offer without award of type DISCOUNT_ITEM_PRICE, skipping this offer",
                        connection_pool,
                        print_bars
                    )
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
