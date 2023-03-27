import re
import usaddress

from nypl_py_utils.functions.log_helper import create_log

logger = create_log('address_helper')

_STREET_KEYS = [
    'StreetNamePreDirectional', 'StreetNamePreModifier', 'StreetNamePreType',
    'StreetName', 'StreetNamePostType', 'StreetNamePostModifier',
    'StreetNamePostDirectional']
_SECONDARY_KEYS = [
    'BuildingName', 'SubaddressType', 'OccupancyType', 'OccupancyIdentifier']
_ADDRESS_TAG_MAP = dict.fromkeys(_STREET_KEYS, 'street')
_ADDRESS_TAG_MAP.update(dict.fromkeys(_SECONDARY_KEYS, 'line2'))


def reformat_malformed_address(address_row):
    """
    Parses the original address and uses the parsed version to set the city,
    region, etc. that should be sent to the geocoders
    """
    address_row['house_number'] = ''
    try:
        parsed_address, _ = usaddress.tag(
            address_row['full_address'], tag_mapping=_ADDRESS_TAG_MAP)
        address_row['city'] = parsed_address.get('PlaceName', '')
        address_row['region'] = parsed_address.get('StateName', '')
        address_row['postal_code'] = parsed_address.get('ZipCode', '')
        address_row['house_number'] = parsed_address.get('AddressNumber', '')
        address_row['street_name'] = parsed_address.get('street', '')
        address_row['address'] = (
            address_row['house_number'] + ' ' + address_row['street_name'] +
            ' ' + parsed_address.get('line2', '')).strip()
    except usaddress.RepeatedLabelError as e:
        for df_field, label in [
                ('city', 'PlaceName'), ('region', 'StateName'),
                ('postal_code', 'ZipCode'), ('house_number', 'AddressNumber')]:
            address_row[df_field] = _combine_repeated_labels(
                e.parsed_string, label) or address_row[df_field]

        address_row['street_name'] = _combine_multilabel_field(
            e.parsed_string, _STREET_KEYS)
        line2 = _combine_multilabel_field(e.parsed_string, _SECONDARY_KEYS)
        address = (address_row['house_number'] + ' ' +
                   address_row['street_name'] + ' ' + line2).strip()
        if len(address) > 0:
            address_row['address'] = address

    # Strip the city and region of anything that's not a letter, space, or -.
    address_row['city'] = re.sub('[^A-Za-zÀ-ÖØ-öø-ÿ-\\s]', '',
                                 address_row['city']).strip()
    address_row['region'] = re.sub('[^A-Za-zÀ-ÖØ-öø-ÿ-\\s]', '',
                                   address_row['region']).strip()
    # Strip the street_name and address of anything that's not a letter, space,
    # digit, or common punctuation.
    address_row['street_name'] = re.sub('[^A-Za-zÀ-ÖØ-öø-ÿ0-9-\\s#&.,;:+@/]',
                                        '', address_row['street_name']).strip()
    address_row['address'] = re.sub('[^A-Za-zÀ-ÖØ-öø-ÿ0-9-\\s#&.,;:+@/]', '',
                                    address_row['address']).strip()
    # Strip the postal_code of anything that's not a digit or -.
    address_row['postal_code'] = re.sub('[^\\d-]', '',
                                        address_row['postal_code']).strip()
    return address_row


def _combine_repeated_labels(parsed_string, label):
    """
    When the parsed address contains multiple portions with the same label,
    concatenate them unless they are identical.

    Returns None if the output is empty
    """
    output_list = []
    for addr_portion in parsed_string:
        if (addr_portion[1] == label
                and addr_portion[0] not in output_list):
            output_list.append(addr_portion[0])
    output = ' '.join(output_list).strip()
    return output if len(output) > 0 else None


def _combine_multilabel_field(parsed_string, labels):
    """
    For repeated labels that all map to the same field (e.g. the multiple
    portions of the street_name), combine them all separately using
    _combine_repeated_labels() and then concatenate them.

    Returns an empty string if the output is empty
    """
    output_list = []
    for label in labels:
        parsed_label = _combine_repeated_labels(
            parsed_string, label)
        if parsed_label is not None and parsed_label not in output_list:
            output_list.append(parsed_label)
    return ' '.join(output_list).strip()
