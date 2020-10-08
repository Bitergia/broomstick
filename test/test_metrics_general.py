# -*- coding: utf-8 -*-
#
# Copyright (C) 2020 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Alberto Pérez García-Plaza <alpgarcia@bitergia.com>
#
import pandas
import sys
import unittest

from pandas.testing import assert_frame_equal
from unittest import TestCase, mock

# Make sure we use our code and not any other could we have installed
sys.path.insert(0, '..')

from broomstick.metrics import general as gm
from broomstick.core import DataSource


class TestMetricsGeneral(TestCase):

    @mock.patch('broomstick.metrics.general.com.contributions_count_total')
    def test_contributions_count_total(
            self,
            contributions_count_total_mock):
        """Test count contributions method.
        """

        # Test with start and end dates
        #

        contributions_count_total_mock.return_value = 1020

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = gm.contributions_count_total(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=True)

        self.assertEqual(result, 1020)

        # Test with start date only
        #

        contributions_count_total_mock.return_value = 2022

        result = gm.contributions_count_total(
            DataSource.GIT,
            start_date=start_date)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=True)

        self.assertEqual(result, 2022)

        # Test with start date and exclude unknown
        #

        contributions_count_total_mock.return_value = 111

        result = gm.contributions_count_total(
            DataSource.GIT,
            start_date=start_date,
            exclude_unknown=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)

        self.assertEqual(result, 111)

        # Test with both dates and exclude unknown
        #

        contributions_count_total_mock.return_value = 9990

        result = gm.contributions_count_total(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        self.assertEqual(result, 9990)

    @mock.patch('broomstick.metrics.general.com.contributions_count_unknown')
    def test_contributions_count_unknown(
            self,
            contributions_count_unknown_mock):
        """Test count contributions sent by people affiliated to Unknown method.
        """

        # Test with start and end dates
        #

        contributions_count_unknown_mock.return_value = 1020

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = gm.contributions_count_unknown(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        contributions_count_unknown_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        self.assertEqual(result, 1020)

        # Test with start date only
        #

        contributions_count_unknown_mock.return_value = 3333

        result = gm.contributions_count_unknown(
            DataSource.GIT,
            start_date=start_date)

        contributions_count_unknown_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None)

        self.assertEqual(result, 3333)

    @mock.patch('broomstick.metrics.general.com.contributions_count_total')
    @mock.patch('broomstick.metrics.general.com.contributions_count_unknown')
    def test_contributions_unknown_percentage(
            self,
            contributions_count_unknown_mock,
            contributions_count_total_mock):
        """Test `contributions_unknown_percentage` method.
        """

        # Test with start and end dates
        #

        contributions_count_total_mock.return_value = 1000
        contributions_count_unknown_mock.return_value = 500

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = gm.contributions_unknown_percentage(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)
        contributions_count_unknown_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        self.assertEqual(result, 50)

        # Test with start and end dates
        #

        contributions_count_total_mock.return_value = 1000
        contributions_count_unknown_mock.return_value = 20

        result = gm.contributions_unknown_percentage(
            DataSource.GIT,
            start_date=start_date)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)
        contributions_count_unknown_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None)

        self.assertEqual(result, 2)

    @mock.patch('broomstick.metrics.general.com.contributions_count_by_org')
    def test_contributions_count_by_org(
            self,
            contributions_count_by_org_mock):
        """Test count contributions by organization method.
        """

        expected_data = {
            'organization': ['Lled', 'Marble', 'Nanosoft'],
            'contributions': [179, 125, 30]
        }

        expected_df = pandas.DataFrame(
            expected_data,
            columns=['organization', 'contributions'])

        # Test with start and end dates
        #

        contributions_count_by_org_mock.return_value = expected_df

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = gm.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=True)

        assert_frame_equal(result, expected_df)

        # Test with start date only
        #

        result = gm.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date)

        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=True)

        assert_frame_equal(result, expected_df)

        # Test with start date and exclude unknown
        #

        result = gm.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date,
            exclude_unknown=False)

        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)

        assert_frame_equal(result, expected_df)

        # Test with both dates and exclude unknown
        #

        result = gm.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        assert_frame_equal(result, expected_df)


    @mock.patch('broomstick.metrics.general.com.contributions_count_by_contributor')
    def test_contributions_count_by_contributor(
            self,
            contributions_count_by_contributor_mock):
        """Test count contributions by contributor method.
        """

        expected_data = {
            'contributor': ['Anne', 'Bob', 'Carl'],
            'contributions': [179, 125, 30]
        }

        expected_df = pandas.DataFrame(
            expected_data,
            columns=['contributor', 'contributions'])

        # Test with start and end dates
        #

        contributions_count_by_contributor_mock.return_value = expected_df

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = gm.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        contributions_count_by_contributor_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_bots=True)

        assert_frame_equal(result, expected_df)

        # Test with start date only
        #

        result = gm.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date)

        contributions_count_by_contributor_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_bots=True)

        assert_frame_equal(result, expected_df)

        # Test with start date and exclude bots
        #

        result = gm.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date,
            exclude_bots=False)

        contributions_count_by_contributor_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_bots=False)

        assert_frame_equal(result, expected_df)

        # Test with both dates and exclude bots
        #

        result = gm.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_bots=False)

        contributions_count_by_contributor_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_bots=False)

        assert_frame_equal(result, expected_df)

if __name__ == '__main__':
    unittest.main()
