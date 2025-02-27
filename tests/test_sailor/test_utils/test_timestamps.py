import datetime

import pytest
import pandas as pd

from sailor.utils.timestamps import _any_to_timestamp, _calculate_nice_sub_intervals, _timestamp_to_date_string


@pytest.mark.parametrize('testdescription,input,expected', [
    ('iso8601 string',
        '2021-01-01 18:00:00+02:00',
        pd.Timestamp(year=2021, month=1, day=1, hour=16, minute=0, second=0, tz='UTC')),
    ('datetime.datetime',
        datetime.datetime(year=2021, month=1, day=1, hour=16, minute=0, second=0),
        pd.Timestamp(year=2021, month=1, day=1, hour=16, minute=0, second=0, tz='UTC')),
    ('pd.Timestamp',
        pd.Timestamp(year=2021, month=1, day=1, hour=16, minute=0, second=0),
        pd.Timestamp(year=2021, month=1, day=1, hour=16, minute=0, second=0, tz='UTC')),
    ('datetime.date',
        datetime.date(year=2021, month=1, day=1),
        pd.Timestamp(year=2021, month=1, day=1, tz='UTC'))
])
@pytest.mark.filterwarnings('ignore:Trying to parse non-timezone-aware timestamp, assuming UTC.')
def test_any_to_timestamp_types(input, expected, testdescription):
    actual = _any_to_timestamp(input)
    assert actual == expected


def test_calculate_nice_sub_intervals_short_interval_does_not_raise():
    _calculate_nice_sub_intervals(pd.Timedelta('500ms'), 10)


def test_calculate_nice_sub_intervals_single_break_does_not_raise():
    _calculate_nice_sub_intervals(pd.Timedelta('1D'), 1)


@pytest.mark.parametrize('testdescr,input,expected,expect_warning', [
    ('produces warning', pd.Timestamp(year=2021, month=1, day=1, hour=2, minute=0, second=0),
        '2021-01-01', True),
    ('handles timezone', pd.Timestamp(year=2021, month=1, day=1, hour=2, minute=0, second=0, tz="UTC+0400"),
        '2020-12-31', True),
    ('without time component', pd.Timestamp(year=2021, month=1, day=1),
        '2021-01-01', False)
])
def test_timestamp_to_date_string(input, expected, expect_warning, testdescr):
    if expect_warning:
        with pytest.warns(UserWarning):
            actual = _timestamp_to_date_string(input)
    else:
        with pytest.warns(None) as record:
            actual = _timestamp_to_date_string(input)
            if len(record) > 0:
                pytest.fail('Did not expect a warning.')
    assert actual == expected
