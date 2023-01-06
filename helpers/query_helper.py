import os

_NEW_PATRONS_QUERY = '''
    SELECT
        x.id, ptype_code, pcode3, home_library_code,
        TRIM(city), TRIM(region), TRIM(postal_code), TRIM(addr1),
        (activity_gmt AT TIME ZONE 'EST')::DATE,
        deletion_date_gmt,
        (record_last_updated_gmt AT TIME ZONE 'EST')::DATE,
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
        deletion_date_gmt,
        (creation_date_gmt AT TIME ZONE 'EST')::DATE,
        record_last_updated_gmt
    FROM (
        SELECT
            id, record_last_updated_gmt, deletion_date_gmt, creation_date_gmt
        FROM sierra_view.record_metadata
        WHERE record_type_code = 'p'
            AND record_last_updated_gmt >= '{cached_update_dt}'
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
    ORDER BY deletion_date_gmt
    LIMIT {limit};'''

_REDSHIFT_ADDRESS_QUERY = '''
    SELECT address_hash, patron_id, geoid
    FROM public.{redshift_table}
    WHERE address_hash IN ({address_hashes})
'''

_REDSHIFT_PATRON_QUERY = '''
    SELECT patron_id, address_hash, postal_code, geoid, creation_date_et,
        circ_active_date_et, ptype_code, pcode3, patron_home_library_code
    FROM public.{redshift_table}
    WHERE patron_id IN ({patron_ids})
'''


def build_new_patrons_query(creation_dt_start):
    return _NEW_PATRONS_QUERY.format(
        cached_creation_dt=creation_dt_start,
        limit=os.environ['SIERRA_BATCH_SIZE'],
        total_limit=int(os.environ['SIERRA_BATCH_SIZE']) * 2)


def build_updated_patrons_query(update_dt_start):
    return _UPDATED_PATRONS_QUERY.format(
        cached_update_dt=update_dt_start,
        limit=os.environ['SIERRA_BATCH_SIZE'],
        total_limit=int(os.environ['SIERRA_BATCH_SIZE']) * 2)


def build_deleted_patrons_query(deletion_date_start):
    return _DELETED_PATRONS_QUERY.format(
        cached_deletion_date=deletion_date_start,
        limit=os.environ['SIERRA_BATCH_SIZE'])


def build_redshift_address_query(address_hashes):
    return _REDSHIFT_ADDRESS_QUERY.format(
        redshift_table=os.environ['REDSHIFT_TABLE'],
        address_hashes=address_hashes)


def build_redshift_patron_query(patron_ids):
    return _REDSHIFT_PATRON_QUERY.format(
        redshift_table=os.environ['REDSHIFT_TABLE'],
        patron_ids=patron_ids)
