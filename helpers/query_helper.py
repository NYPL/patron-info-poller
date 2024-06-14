import os

from helpers.pipeline_mode import PipelineMode

_ACTIVE_PATRONS_QUERY = '''
    SELECT
        x.id, ptype_code, pcode3,
        CASE WHEN LENGTH(TRIM(home_library_code)) = 0
            OR TRIM(home_library_code) = 'none' THEN NULL
            ELSE TRIM(home_library_code) END,
        TRIM(city), TRIM(region), TRIM(postal_code), TRIM(addr1),
        TO_DATE(CAST(activity_gmt AS TEXT), 'YYYY-MM-DD'),
        deletion_date_gmt,
        record_last_updated_gmt,
        creation_date_gmt
    FROM (
        SELECT
            id, record_last_updated_gmt, deletion_date_gmt, creation_date_gmt
        FROM sierra_view.record_metadata
        WHERE record_type_code = 'p'
            AND {ordering_field} >= '{start_dt}'
            AND {ordering_field} < '{now}'
            AND {ordering_field} IS NOT NULL
        ORDER BY {ordering_field}
        LIMIT {limit}) x
    LEFT JOIN sierra_view.patron_record_address
        ON x.id = patron_record_address.patron_record_id
    LEFT JOIN sierra_view.patron_view
        ON x.id = patron_view.id
    ORDER BY {ordering_field}, display_order, patron_record_address_type_id;'''

_DELETED_PATRONS_QUERY = '''
    SELECT id, deletion_date_gmt
    FROM sierra_view.record_metadata
    WHERE record_type_code = 'p'
        AND deletion_date_gmt >= '{cached_deletion_date}'
        AND deletion_date_gmt < '{now}'
        AND deletion_date_gmt IS NOT NULL
    ORDER BY deletion_date_gmt
    LIMIT {limit};'''

_REDSHIFT_ADDRESS_QUERY = '''
    SELECT address_hash, patron_id, geoid, initial_patron_home_library_code
    FROM {redshift_table}
    WHERE address_hash IN ({address_hashes})
'''

_REDSHIFT_IPHLC_QUERY = '''
    SELECT patron_id, initial_patron_home_library_code
    FROM {redshift_table}
    WHERE patron_id IN ({patron_ids})
'''

_REDSHIFT_PATRON_QUERY = '''
    SELECT patron_id, address_hash, postal_code, geoid, creation_date_et,
        circ_active_date_et, ptype_code, pcode3, patron_home_library_code,
        initial_patron_home_library_code
    FROM {redshift_table}
    WHERE patron_id IN ({patron_ids})
'''


def build_active_patrons_query(mode, poller_state, now):
    if mode == PipelineMode.NEW_PATRONS:
        ordering_field = 'creation_date_gmt'
        start_dt = poller_state['creation_dt']
    elif mode == PipelineMode.UPDATED_PATRONS:
        ordering_field = 'record_last_updated_gmt'
        start_dt = poller_state['update_dt']
    return _ACTIVE_PATRONS_QUERY.format(
        ordering_field=ordering_field, start_dt=start_dt, now=now,
        limit=os.environ['ACTIVE_PATRON_BATCH_SIZE'])


def build_deleted_patrons_query(deletion_date_start, now):
    return _DELETED_PATRONS_QUERY.format(
        cached_deletion_date=deletion_date_start,
        limit=os.environ['DELETED_PATRON_BATCH_SIZE'],
        now=now)


def build_redshift_address_query(address_hashes):
    return _REDSHIFT_ADDRESS_QUERY.format(
        redshift_table=os.environ['REDSHIFT_TABLE'],
        address_hashes=address_hashes)


def build_redshift_iphlc_query(patron_ids):
    return _REDSHIFT_IPHLC_QUERY.format(
        redshift_table=os.environ['REDSHIFT_TABLE'],
        patron_ids=patron_ids)


def build_redshift_patron_query(patron_ids):
    return _REDSHIFT_PATRON_QUERY.format(
        redshift_table=os.environ['REDSHIFT_TABLE'],
        patron_ids=patron_ids)
