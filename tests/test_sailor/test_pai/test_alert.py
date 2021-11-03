from unittest.mock import patch

import pytest

from sailor.pai import constants
from sailor import pai
from sailor.pai.utils import _PredictiveAssetInsightsField
from sailor.pai.alert import Alert, AlertSet


@pytest.fixture
def make_alert():
    def maker(**kwargs):
        kwargs.setdefault('AlertId', 'id')
        kwargs.setdefault('AlertType', 'alert_type')
        return Alert(kwargs)
    return maker


@pytest.fixture
def make_alert_set(make_alert):
    def maker(**kwargs):
        alert_defs = [dict() for _ in list(kwargs.values())[0]]
        for k, values in kwargs.items():
            for i, value in enumerate(values):
                alert_defs[i][k] = value
        return AlertSet([make_alert(**x) for x in alert_defs])
    return maker


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

    def test_custom_properties_uses_startswith_z(self):
        alert = Alert({'AlertId': 'id',
                       'Z_mycustom': 'mycustom', 'z_another': 'another'})
        assert alert._custom_properties == {'Z_mycustom': 'mycustom', 'z_another': 'another'}

    def test_custom_properties_are_set_as_attributes(self):
        alert = Alert({'AlertId': 'id',
                       'Z_mycustom': 'mycustom', 'z_another': 'another'})
        assert alert.id == 'id'
        assert alert.Z_mycustom == 'mycustom'
        assert alert.z_another == 'another'


@pytest.mark.parametrize('testdesc,kwargs,expected_cols', [
    ('default=all noncustom properties',
        dict(), ['id', 'type']),
    ('only specified columns',
        dict(columns=['id', 'Z_mycustom']), ['id', 'Z_mycustom']),
    ('all properties AND all custom properties',
        dict(include_all_custom_properties=True), ['id', 'type', 'Z_mycustom', 'z_another']),
    ('specified AND all custom properties',
        dict(columns=['id', 'Z_mycustom'], include_all_custom_properties=True), ['id', 'Z_mycustom', 'z_another'])
])
def test_alertset_as_df_expects_columns(make_alert_set, monkeypatch,
                                        kwargs, expected_cols, testdesc):
    monkeypatch.setattr(Alert, '_field_map', {
        'id': _PredictiveAssetInsightsField('id', 'AlertId'),
        'type': _PredictiveAssetInsightsField('type', 'AlertType'),
    })
    alert_set = make_alert_set(AlertId=['id1', 'id2', 'id3'],
                               Z_mycustom=['cust1', 'cust2', 'cust3'],
                               z_another=['ano1', 'ano2', 'ano3'])
    actual = alert_set.as_df(**kwargs)
    assert actual.columns.to_list() == expected_cols


def test_alertset_as_df_raises_on_custom_properties_with_multiple_types(make_alert_set, monkeypatch):
    monkeypatch.setattr(Alert, '_field_map', {
        'id': _PredictiveAssetInsightsField('id', 'AlertId'),
        'type': _PredictiveAssetInsightsField('type', 'AlertType'),
    })
    alert_set = make_alert_set(AlertId=['id1', 'id2', 'id3'],
                               AlertType=['type', 'type', 'DIFFERENT_TYPE'],
                               Z_mycustom=['cust1', 'cust2', 'cust3'],
                               z_another=['ano1', 'ano2', 'ano3'])
    with pytest.raises(RuntimeError, match='More than one alert type present in result'):
        alert_set.as_df(include_all_custom_properties=True)
