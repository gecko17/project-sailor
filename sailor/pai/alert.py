"""
Retrieve Alert information from the alert re-use service.

Classes are provided for individual Alert as well as groups of Alerts (AlertSet).
"""

from functools import cache
import re

import sailor.assetcentral.utils
from sailor import _base
from sailor.utils.oauth_wrapper import get_oauth_client
from sailor.utils.timestamps import _odata_to_timestamp_parser
from sailor._base.masterdata import _qt_odata_datetimeoffset, _qt_double
from .constants import ALERTS_READ_PATH, ALERTS_WRITE_PATH
from .utils import (PredictiveAssetInsightsEntity, _PredictiveAssetInsightsField,
                    PredictiveAssetInsightsEntitySet, _pai_application_url, _pai_fetch_data)

_ALERT_FIELDS = [
    _PredictiveAssetInsightsField('description', 'Description', 'description'),
    _PredictiveAssetInsightsField('severity_code', 'SeverityCode', 'severityCode', is_mandatory=True,
                                  query_transformer=_qt_double),
    _PredictiveAssetInsightsField('category', 'Category'),
    _PredictiveAssetInsightsField('equipment_name', 'EquipmentName'),
    _PredictiveAssetInsightsField('model_name', 'ModelName'),
    _PredictiveAssetInsightsField('indicator_name', 'IndicatorName'),
    _PredictiveAssetInsightsField('indicator_group_name', 'IndicatorGroupName'),
    _PredictiveAssetInsightsField('template_name', 'TemplateName'),
    _PredictiveAssetInsightsField('count', 'Count', query_transformer=_qt_double),
    _PredictiveAssetInsightsField('status_code', 'StatusCode', query_transformer=_qt_double),
    _PredictiveAssetInsightsField('triggered_on', 'TriggeredOn', 'triggeredOn', is_mandatory=True,
                                  get_extractor=_odata_to_timestamp_parser(),
                                  query_transformer=_qt_odata_datetimeoffset),
    _PredictiveAssetInsightsField('last_occured_on', 'LastOccuredOn', get_extractor=_odata_to_timestamp_parser(),
                                  query_transformer=_qt_odata_datetimeoffset),
    _PredictiveAssetInsightsField('type_description', 'AlertTypeDescription'),
    _PredictiveAssetInsightsField('error_code_description', 'ErrorCodeDescription'),
    _PredictiveAssetInsightsField('type', 'AlertType', 'alertType', is_mandatory=True),
    _PredictiveAssetInsightsField('source', 'Source', 'source'),
    _PredictiveAssetInsightsField('id', 'AlertId'),
    _PredictiveAssetInsightsField('equipment_id', 'EquipmentID', 'equipmentId', is_mandatory=True),
    _PredictiveAssetInsightsField('model_id', 'ModelID'),
    _PredictiveAssetInsightsField('template_id', 'TemplateID', 'templateId'),
    _PredictiveAssetInsightsField('indicator_id', 'IndicatorID', 'indicatorId'),
    _PredictiveAssetInsightsField('indicator_group_id', 'IndicatorGroupID', 'indicatorGroupId'),
    _PredictiveAssetInsightsField('notification_id', 'NotificationId'),
    _PredictiveAssetInsightsField('error_code_id', 'ErrorCodeID', 'errorCodeId'),
    _PredictiveAssetInsightsField('_indicator_description', 'IndicatorDescription'),
    _PredictiveAssetInsightsField('_country_id', 'CountryID'),
    _PredictiveAssetInsightsField('_functional_location_id', 'FunctionalLocationID'),
    _PredictiveAssetInsightsField('_maintenance_plant', 'MaintenancePlant'),
    _PredictiveAssetInsightsField('_functional_location_description', 'FunctionalLocationDescription'),
    _PredictiveAssetInsightsField('_top_functional_location_name', 'TopFunctionalLocationName'),
    _PredictiveAssetInsightsField('_planner_group', 'PlannerGroup'),
    _PredictiveAssetInsightsField('_ref_alert_type_id', 'RefAlertTypeId'),
    _PredictiveAssetInsightsField('_operator_name', 'OperatorName'),
    _PredictiveAssetInsightsField('_created_by', 'CreatedBy'),
    _PredictiveAssetInsightsField('_changed_by', 'ChangedBy'),
    _PredictiveAssetInsightsField('_serial_number', 'SerialNumber'),
    _PredictiveAssetInsightsField('_changed_on', 'ChangedOn', get_extractor=_odata_to_timestamp_parser(),
                                  query_transformer=_qt_odata_datetimeoffset),
    _PredictiveAssetInsightsField('_processor', 'Processor'),
    _PredictiveAssetInsightsField('_top_equipment_id', 'TopEquipmentID'),
    _PredictiveAssetInsightsField('_planning_plant', 'PlanningPlant'),
    _PredictiveAssetInsightsField('_operator_id', 'OperatorID'),
    _PredictiveAssetInsightsField('_top_equipment_name', 'TopEquipmentName'),
    _PredictiveAssetInsightsField('_created_on', 'CreatedOn', get_extractor=_odata_to_timestamp_parser(),
                                  query_transformer=_qt_odata_datetimeoffset),
    _PredictiveAssetInsightsField('_model_description', 'ModelDescription'),
    _PredictiveAssetInsightsField('_top_equipment_description', 'TopEquipmentDescription'),
    _PredictiveAssetInsightsField('_functional_location_name', 'FunctionalLocationName'),
    _PredictiveAssetInsightsField('_top_functional_location_description', 'TopFunctionalLocationDescription'),
    _PredictiveAssetInsightsField('_top_functional_location_id', 'TopFunctionalLocationID'),
    _PredictiveAssetInsightsField('_equipment_description', 'EquipmentDescription'),
]


@_base.add_properties
class Alert(PredictiveAssetInsightsEntity):
    """PredictiveAssetInsights Alert Object."""

    _field_map = {field.our_name: field for field in _ALERT_FIELDS}

    def __init__(self, ac_json: dict):
        super().__init__(ac_json)
        for key, value in self._custom_properties.items():
            setattr(self, key, value)

    @property
    @cache
    def _custom_properties(self):
        return {key: value for key, value in self.raw.items()
                if key.startswith('Z_') or key.startswith('z_')}


class AlertSet(PredictiveAssetInsightsEntitySet):
    """Class representing a group of Alerts."""

    _element_type = Alert
    _method_defaults = {
        'plot_distribution': {
            'by': 'type',
        },
    }

    def as_df(self, columns=None, include_all_custom_properties=False):
        """Return all information on the objects stored in the AlertSet as a pandas dataframe.

        ``include_all_custom_properties`` can be set to True to add ALL custom properties attached to the alerts
        to the resulting DataFrame. This can only be used when all alerts in the AlertSet are of the same type.
        """
        if columns is None:
            columns = [field.our_name for field in self._element_type._field_map.values() if field.is_exposed]

        df = super().as_df(columns=columns + ['type'])  # type might be needed and will be removed at the end

        if len(self) > 0 and include_all_custom_properties:
            if df['type'].nunique() > 1:
                raise RuntimeError('Cannot include custom properties: More than one alert type present in result.')
            custom_columns = list(self[0]._custom_properties.keys()) + ['id']
            df_custom = super().as_df(columns=custom_columns)
            df = df.merge(df_custom)

        if 'type' not in columns:
            df.drop(columns='type', inplace=True)

        return df


def find_alerts(*, extended_filters=(), **kwargs) -> AlertSet:
    """
    Fetch Alerts from PredictiveAssetInsights (PAI) with the applied filters, return an AlertSet.

    This method supports the common filter language explained at :ref:`filter`.

    Parameters
    ----------
    extended_filters
        See :ref:`filter`.
    **kwargs
        See :ref:`filter`.

    Examples
    --------
    Get all Alerts with the type 'MyAlertType'::

        find_alerts(type='MyAlertType')

    Get all Error(severity code=10) and Information(severity code=1) alerts::

        find_equipment(severity_code=[10, 1])
    """
    unbreakable_filters, breakable_filters = \
        _base.parse_filter_parameters(kwargs, extended_filters, Alert._field_map)

    endpoint_url = _pai_application_url() + ALERTS_READ_PATH
    object_list = _pai_fetch_data(endpoint_url, unbreakable_filters, breakable_filters)
    return AlertSet([Alert(obj) for obj in object_list])
