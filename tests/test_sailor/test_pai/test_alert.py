from unittest.mock import patch, call, MagicMock

import pytest

from sailor.pai import constants
from sailor import pai
from sailor.pai.alert import Alert, create_alert


@pytest.fixture
def mock_ac_url():
    with patch('sailor.assetcentral.utils._ac_application_url') as mock:
        mock.return_value = 'ac_base_url'
        yield mock


@pytest.fixture
def mock_pai_url():
    with patch('sailor.pai.alert._pai_application_url') as mock:
        mock.return_value = 'pai_base_url'
        yield mock


def get_parameters(test_object):
    test_params = {
        'alert': {
            'function': pai.find_alerts,
            'set_class': pai.alert.AlertSet,
            'id_field': 'AlertId',
            'endpoint': constants.ALERTS_READ_PATH
        },
    }
    return test_params[test_object]


class TestAlert():

    @pytest.mark.filterwarnings('ignore:Following parameters are not in our terminology')
    def test_find_alerts_expect_fetch_call_args(self):
        params = get_parameters('alert')

        find_params = dict(extended_filters=['unknown_integer_param < 10'],
                           unknown_string_param=["'Type A'", "'Type F'"])
        expected_call_args = (['unknown_integer_param lt 10'],
                              [["unknown_string_param eq 'Type A'", "unknown_string_param eq 'Type F'"]])

        fetch_result = [{'AlertId': 'test_id1'}, {'AlertId': 'test_id2'}]
        instance_class = params['set_class']._element_type

        objects = [instance_class({params['id_field']: x}) for x in ['test_id1', 'test_id2']]
        expected_result = params['set_class'](objects)

        with patch('sailor.pai.alert._pai_application_url') as mock:
            mock.return_value = 'base_url'
            with patch('sailor.pai.alert._pai_fetch_data') as mock_fetch:
                mock_fetch.return_value = fetch_result
                actual_result = params['function'](**find_params)

        assert params['endpoint'] in mock_fetch.call_args.args[0]
        assert mock_fetch.call_args.args[1:] == expected_call_args
        assert actual_result == expected_result

    def test_expected_public_attributes_are_present(self):
        expected_attributes = [
            'description', 'severity_code', 'category', 'equipment_name', 'model_name', 'indicator_name',
            'indicator_group_name', 'template_name', 'count', 'status_code', 'triggered_on', 'last_occured_on',
            'type_description', 'error_code_description', 'type', 'source', 'id', 'equipment_id', 'model_id',
            'template_id', 'indicator_id', 'indicator_group_id', 'notification_id', 'error_code_id',
        ]

        fieldmap_public_attributes = [
            field.our_name for field in Alert._field_map.values() if field.is_exposed
        ]

        assert expected_attributes == fieldmap_public_attributes



@pytest.mark.filterwarnings('ignore:Unknown name for _AlertWriteRequest parameter found')
def test_create_alert_create_calls_and_result(mock_ac_url, mock_pai_url, mock_request):
    input_kwargs = {'param1': 'abc123', 'param2': 'def456'}
    mock_post_response = b'12345678-1234-1234-1234-1234567890ab'
    mock_get_response = {'d': {'results': [{'some': 'result'}]}}
    mock_request.side_effect = [mock_post_response, mock_get_response]
    expected_request_dict = input_kwargs

    # mock validate so that validation does not fail
    with patch('sailor.assetcentral.utils._AssetcentralWriteRequest.validate'):
        actual = create_alert(**input_kwargs)

    mock_request.assert_has_calls([
        call('POST', 'ac_base_url' + constants.ALERTS_WRITE_PATH, json=expected_request_dict),
        call('GET', 'pai_base_url' + constants.ALERTS_READ_PATH,
             params={'$filter': "AlertId eq '12345678-1234-1234-1234-1234567890ab'", '$format': 'json'})])
    assert type(actual) == Alert
    assert actual.raw == {'some': 'result'}


# TODO: this test might be able to be turned into a generic test for all _create_or_update functions
@pytest.mark.parametrize('find_call_result', [
    ({'d': {'results': []}}),
    ({'d': {'results': [{'AlertId': '123'}, {'AlertId': '456'}]}}),
])
@pytest.mark.filterwarnings('ignore::sailor.utils.utils.DataNotFoundWarning')
@patch('sailor.pai.alert._AlertWriteRequest')
def test_generic_create_update_raises_when_find_has_no_single_result(mock_wr, mock_pai_url, mock_ac_url, mock_request, find_call_result):
    successful_create_result = b'12345678-1234-1234-1234-1234567890ab'
    mock_request.side_effect = [successful_create_result, find_call_result]

    with pytest.raises(RuntimeError, match='Unexpected error'):
        create_alert()


# def test_create_notification_integration(mock_url, mock_request):
#     create_kwargs = {'equipment_id': 'XYZ', 'notification_type': 'M2',
#                      'short_description': 'test', 'priority': 15, 'status': 'NEW'}
#     mock_post_response = {'notificationID': '123'}
#     mock_get_response = {'equipmentId': 'XYZ', 'notificationId': '123', 'notificationType': 'M2',
#                          'shortDescription': 'test', 'priority': 15, 'status': 'NEW'}
#     mock_request.side_effect = [mock_post_response, mock_get_response]
#     expected_request_dict = {
#         'equipmentID': 'XYZ', 'type': 'M2', 'description': {'shortDescription': 'test'},
#         'priority': 15, 'status': ['NEW']}

#     actual = create_notification(**create_kwargs)

#     mock_request.assert_has_calls([
#         call('POST', 'base_url/services/api/v1/notification', json=expected_request_dict),
#         call('GET', 'base_url/services/api/v1/notification', params={'$filter': "notificationId eq '123'",
#                                                                      '$format': 'json'})])
#     assert type(actual) == Notification
#     for property_name, value in create_kwargs.items():
#         assert getattr(actual, property_name) == value