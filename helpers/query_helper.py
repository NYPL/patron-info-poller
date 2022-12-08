import os

_NEW_PATRONS_QUERY = '''
    SELECT
        record_metadata.id, ptype_code, pcode3, home_library_code,
        TRIM(city) AS city,
        TRIM(region) AS region,
        TRIM(postal_code) AS postal_code,
        TRIM(addr1) AS address,
        (activity_gmt AT TIME ZONE 'EST')::DATE AS circ_active_date_et,
        (record_last_updated_gmt AT TIME ZONE 'EST')::DATE
            AS last_updated_date_et,
        deletion_date_gmt AS deletion_date_et,
        creation_date_gmt AS creation_timestamp
    FROM sierra_view.record_metadata
    LEFT JOIN sierra_view.patron_record_address
        ON record_metadata.id = patron_record_address.patron_record_id
    LEFT JOIN sierra_view.patron_view
        ON record_metadata.id = patron_view.id
    WHERE record_metadata.record_type_code = 'p'
        AND creation_date_gmt >= '{cached_creation_dt}'
    ORDER BY creation_date_gmt
    LIMIT {limit};'''

_UPDATED_PATRONS_QUERY = '''
    SELECT
        record_metadata.id, ptype_code, pcode3, home_library_code,
        TRIM(city) AS city,
        TRIM(region) AS region,
        TRIM(postal_code) AS postal_code,
        TRIM(addr1) AS address,
        (activity_gmt AT TIME ZONE 'EST')::DATE AS circ_active_date_et,
        (creation_date_gmt AT TIME ZONE 'EST')::DATE AS creation_date_et,
        deletion_date_gmt AS deletion_date_et,
        record_last_updated_gmt AS last_updated_timestamp
    FROM sierra_view.record_metadata
    LEFT JOIN sierra_view.patron_record_address
        ON record_metadata.id = patron_record_address.patron_record_id
    LEFT JOIN sierra_view.patron_view
        ON record_metadata.id = patron_view.id
    WHERE record_metadata.record_type_code = 'p'
        AND record_last_updated_gmt >= '{cached_update_dt}'
        AND record_metadata.id NOT IN ({new_patron_ids})
    ORDER BY record_last_updated_gmt
    LIMIT {limit};'''

_DELETED_PATRONS_QUERY = '''
    SELECT id, deletion_date_gmt AS deletion_date_et
    FROM sierra_view.record_metadata
    WHERE record_type_code = 'p'
        AND deletion_date_gmt >= '{cached_deletion_date}'
        AND id NOT IN ({processed_patron_ids})
    ORDER BY deletion_date_gmt
    LIMIT {limit};'''


def build_new_patrons_query(creation_dt_start):
    return _NEW_PATRONS_QUERY.format(cached_creation_dt=creation_dt_start,
                                     limit=os.environ['SIERRA_BATCH_SIZE'])


def build_updated_patrons_query(update_dt_start, new_patron_ids):
    return _UPDATED_PATRONS_QUERY.format(
        cached_update_dt=update_dt_start,
        new_patron_ids=str(new_patron_ids)[1:-1],
        limit=os.environ['SIERRA_BATCH_SIZE'])


def build_deleted_patrons_query(deletion_date_start, processed_patron_ids):
    return _DELETED_PATRONS_QUERY.format(
        cached_deletion_date=deletion_date_start,
        processed_patron_ids=str(processed_patron_ids)[1:-1],
        limit=os.environ['SIERRA_BATCH_SIZE'])
