import re

from nypl_py_utils.functions.log_helper import create_log

logger = create_log('address_helper')

_ENDS_WITH_POSTAL_CODE = re.compile(r'(^|\s)(\d{5}([\-]?\d{4})?)$')
_ENDS_WITH_NY_BOROUGH = re.compile(
    r'(^|\s)(BRONX|BROOKLYN|MANHATTAN|QUEENS|STATEN ISLAND)($|\s(NY|NYC|NEW YORK)$)')  # noqa: E501'
_ENDS_WITH_NY_NY = re.compile(
    r'(^|\s)(NY|NYC|NEW YORK)\s(NY|NYC|NEW YORK)$')
_CONTAINS_NY_STATE = re.compile(r'(^|\s)(NY|NYC|NEW YORK)($|\s)')


def reformat_malformed_addresses(address_row):
    """
    Looks for common errors in a Sierra address and corrects them:

    1) If the postal_code field does not contain a correct postal code and one
    of the other fields ends with a string resembling a postal code, use that
    instead. Then remove anything resembling a postal code from the end of all
    the other fields.
    2) If the city field is empty and one of the other fields contains a
    borough name, use that as the city and set the region to 'NY'
    3) If the region field is still empty and one of the other fields contains
    some variation of 'NY', use that instead. We do not check for other state
    abbreviations because many are also common address abbreviations (e.g FL
    for floor, LA for lane, etc.). Then remove any borough or variation of 'NY'
    from the other fields as long as that doesn't result in an empty string.
    4) Remove anything that's not a digit or '-' from the postal code
    5) Remove anything that's not a letter, a space, or '- 'from the city and
    region
    6) Remove anything that's not a letter, a space, or '-', '#', '&', '.',
    ',', ';', ':', '+', '@', '/' from the address
    """
    processed_address_row = address_row.fillna('').str.strip()
    if (processed_address_row == '').all():
        return processed_address_row

    processed_address_row = processed_address_row.str.upper()
    processed_address_row['original_postal_code'] = processed_address_row[
        'postal_code']
    processed_address_row['original_city'] = processed_address_row['city']

    # Step 1) in method description
    postal_code_match = _ENDS_WITH_POSTAL_CODE.search(
        processed_address_row['postal_code'])
    if postal_code_match:
        processed_address_row['postal_code'] = postal_code_match.group(
            0).lstrip()
    else:
        for col_name in ['address', 'region', 'city']:
            has_match = _ENDS_WITH_POSTAL_CODE.search(
                processed_address_row[col_name])
            if has_match:
                processed_address_row['postal_code'] = has_match.group(
                    0).lstrip()
                break
    for col_name in ['address', 'region', 'city']:
        processed_address_row[col_name] = re.sub(
            _ENDS_WITH_POSTAL_CODE, '', processed_address_row[col_name]
        ).strip()

    # Step 2) in method description
    borough_match = _ENDS_WITH_NY_BOROUGH.search(
        processed_address_row['city'])
    ny_ny_match = _ENDS_WITH_NY_NY.search(processed_address_row['city'])
    if borough_match:
        processed_address_row['city'] = re.sub(
            _CONTAINS_NY_STATE, '', borough_match.group(0)).strip()
        processed_address_row['region'] = 'NY'
    elif ny_ny_match:
        processed_address_row['city'] = 'NEW YORK'
        processed_address_row['region'] = 'NY'
    elif processed_address_row['city'] == '':
        for col_name in ['address', 'region', 'original_postal_code']:
            has_borough_match = _ENDS_WITH_NY_BOROUGH.search(
                processed_address_row[col_name])
            has_ny_ny_match = _ENDS_WITH_NY_NY.search(
                processed_address_row[col_name])
            if has_borough_match:
                processed_address_row['city'] = re.sub(
                    _CONTAINS_NY_STATE, '', has_borough_match.group(0)).strip()
                processed_address_row['region'] = 'NY'
                break
            if has_ny_ny_match:
                processed_address_row['city'] = 'NEW YORK'
                processed_address_row['region'] = 'NY'
                break
    if processed_address_row['city'] == 'MANHATTAN':
        processed_address_row['city'] = 'NEW YORK'

    # Step 3) in method description
    ny_state_match = _CONTAINS_NY_STATE.search(processed_address_row['region'])
    if ny_state_match:
        processed_address_row['region'] = 'NY'
    elif processed_address_row['region'] == '':
        for col_name in ['address', 'original_city', 'original_postal_code']:
            has_match = _CONTAINS_NY_STATE.search(
                processed_address_row[col_name])
            if has_match:
                processed_address_row['region'] = 'NY'
                break

    processed_address_row['address'] = re.sub(
        _ENDS_WITH_NY_BOROUGH, '', processed_address_row['address']
    ).strip()
    processed_address_row['address'] = re.sub(
        _ENDS_WITH_NY_NY, '', processed_address_row['address']
    ).strip()
    for col_name in ['address', 'region', 'city', 'postal_code']:
        new_val = re.sub(
            _CONTAINS_NY_STATE, '', processed_address_row[col_name]
        ).strip()
        if new_val != '':
            processed_address_row[col_name] = new_val

    # Steps 4)-6) in method description
    processed_address_row['postal_code'] = re.sub(
        '[^\\d-]', '', processed_address_row['postal_code']).strip()
    processed_address_row['city'] = re.sub(
        '[^A-Za-zÀ-ÖØ-öø-ÿ-\\s]', '', processed_address_row['city']).strip()
    processed_address_row['region'] = re.sub(
        '[^A-Za-zÀ-ÖØ-öø-ÿ-\\s]', '', processed_address_row['region']).strip()
    processed_address_row['address'] = re.sub(
        '[^A-Za-zÀ-ÖØ-öø-ÿ0-9-\\s#&.,;:+@/]', '',
        processed_address_row['address']).strip()

    results_row = processed_address_row[[
        'address', 'city', 'region', 'postal_code']]
    input_row = address_row.fillna('').str.upper()
    if not input_row.equals(results_row):
        logger.debug((
            'Changed geocoder address input from:\n{input} to:\n{output}')
            .format(input=input_row, output=results_row))

    return processed_address_row[['address', 'city', 'region', 'postal_code']]
