import os

_NEW_PATRONS_QUERY = '''
    SELECT
        x.id, ptype_code, pcode3, home_library_code,
        TRIM(city), TRIM(region), TRIM(postal_code), TRIM(addr1),
        (activity_gmt AT TIME ZONE 'EST')::DATE,
        (record_last_updated_gmt AT TIME ZONE 'EST')::DATE,
        deletion_date_gmt,
        creation_date_gmt
    FROM (
        SELECT
            id, record_last_updated_gmt, deletion_date_gmt, creation_date_gmt
        FROM sierra_view.record_metadata
        WHERE record_type_code = 'p'
            AND creation_date_gmt >= '{cached_creation_dt}'
        ORDER BY creation_date_gmt
        LIMIT {limit}) x
    LEFT JOIN sierra_view.patron_record_address
        ON x.id = patron_record_address.patron_record_id
    LEFT JOIN sierra_view.patron_view
        ON x.id = patron_view.id
    ORDER BY creation_date_gmt, display_order, patron_record_address_type_id
    LIMIT {total_limit};'''

_UPDATED_PATRONS_QUERY = '''
    SELECT
        x.id, ptype_code, pcode3, home_library_code,
        TRIM(city), TRIM(region), TRIM(postal_code), TRIM(addr1),
        (activity_gmt AT TIME ZONE 'EST')::DATE,
        (creation_date_gmt AT TIME ZONE 'EST')::DATE,
        deletion_date_gmt,
        record_last_updated_gmt
    FROM (
        SELECT
            id, record_last_updated_gmt, deletion_date_gmt, creation_date_gmt
        FROM sierra_view.record_metadata
        WHERE record_type_code = 'p'
            AND record_last_updated_gmt >= '{cached_creation_dt}'
            AND id NOT IN ({new_patron_ids})
        ORDER BY record_last_updated_gmt
        LIMIT {limit}) x
    LEFT JOIN sierra_view.patron_record_address
        ON x.id = patron_record_address.patron_record_id
    LEFT JOIN sierra_view.patron_view
        ON x.id = patron_view.id
    ORDER BY
        record_last_updated_gmt, display_order, patron_record_address_type_id
    LIMIT {total_limit};'''

_DELETED_PATRONS_QUERY = '''
    SELECT id, deletion_date_gmt
    FROM sierra_view.record_metadata
    WHERE record_type_code = 'p'
        AND deletion_date_gmt >= '{cached_deletion_date}'
        AND id NOT IN ({processed_patron_ids})
    ORDER BY deletion_date_gmt
    LIMIT {limit};'''


def build_new_patrons_query(creation_dt_start):
    return _NEW_PATRONS_QUERY.format(
        cached_creation_dt=creation_dt_start,
        limit=os.environ['SIERRA_BATCH_SIZE'],
        total_limit=int(os.environ['SIERRA_BATCH_SIZE']) * 2)


def build_updated_patrons_query(update_dt_start, new_patron_ids):
    return _UPDATED_PATRONS_QUERY.format(
        cached_update_dt=update_dt_start,
        new_patron_ids=str(new_patron_ids)[1:-1],
        limit=os.environ['SIERRA_BATCH_SIZE'],
        total_limit=int(os.environ['SIERRA_BATCH_SIZE']) * 2)


def build_deleted_patrons_query(deletion_date_start, processed_patron_ids):
    return _DELETED_PATRONS_QUERY.format(
        cached_deletion_date=deletion_date_start,
        processed_patron_ids=str(processed_patron_ids)[1:-1],
        limit=os.environ['SIERRA_BATCH_SIZE'])
