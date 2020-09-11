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

from unittest import TestCase, mock
from unittest.mock import MagicMock


# Make sure we use our code and not any other could we have installed
sys.path.insert(0, '..')

from broomstick.metrics import factors as fm
from broomstick.core import DataSource


class TestMetricsFactors(TestCase):

    @mock.patch('broomstick.metrics.factors.cufflinks.go_offline')
    @mock.patch('broomstick.metrics.factors.init_notebook_mode')
    @mock.patch('broomstick.metrics.factors.gm.contributions_count_total')
    @mock.patch('broomstick.metrics.factors.gm.contributions_count_by_org')
    def test_elephant_factor_with_print(
            self,
            contributions_count_by_org_mock,
            contributions_count_total_mock,
            init_notebook_mode_mock,
            go_offline_mock):
        """Test elephant factor method with plotly print mode active.
        """

        expected_data = {
            'organization': ['Lled', 'Marble', 'Nanosoft'],
            'contributions': [175, 165, 30]
        }

        expected_df = pandas.DataFrame(
            expected_data,
            columns=['organization', 'contributions'])
        expected_df['contributions'].iplot = MagicMock(return_value='test')

        contributions_count_total_mock.return_value = 175 + 165 + 30

        contributions_count_by_org_mock.return_value = expected_df

        # Test with start and end dates
        #

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = fm.elephant_factor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=True)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=True)
        go_offline_mock.assert_called_with(connected=True)
        init_notebook_mode_mock.assert_called_with(connected=True)
        expected_df['contributions'].iplot.assert_called_with(
            kind='hist',
            xTitle='contributions',
            yTitle='Organizations',
            title='contributions Distribution')

        self.assertEqual(result, 2)

        # Test with start date only
        #

        result = fm.elephant_factor(
            DataSource.ALL,
            start_date=start_date)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None,
            exclude_unknown=True)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None,
            exclude_unknown=True)
        go_offline_mock.assert_called_with(connected=True)
        init_notebook_mode_mock.assert_called_with(connected=True)
        expected_df['contributions'].iplot.assert_called_with(
            kind='hist',
            xTitle='contributions',
            yTitle='Organizations',
            title='contributions Distribution')

        self.assertEqual(result, 2)

        # Test with start date and exclude unknown
        #

        result = fm.elephant_factor(
            DataSource.GIT,
            start_date=start_date,
            exclude_unknown=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)
        go_offline_mock.assert_called_with(connected=True)
        init_notebook_mode_mock.assert_called_with(connected=True)
        expected_df['contributions'].iplot.assert_called_with(
            kind='hist',
            xTitle='contributions',
            yTitle='Organizations',
            title='contributions Distribution')

        self.assertEqual(result, 2)

        # Test with both dates and exclude unknown
        #

        result = fm.elephant_factor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)
        go_offline_mock.assert_called_with(connected=True)
        init_notebook_mode_mock.assert_called_with(connected=True)
        expected_df['contributions'].iplot.assert_called_with(
            kind='hist',
            xTitle='contributions',
            yTitle='Organizations',
            title='contributions Distribution')

        self.assertEqual(result, 2)

    @mock.patch('broomstick.metrics.factors.cufflinks.go_offline')
    @mock.patch('broomstick.metrics.factors.init_notebook_mode')
    @mock.patch('broomstick.metrics.factors.gm.contributions_count_total')
    @mock.patch('broomstick.metrics.factors.gm.contributions_count_by_org')
    def test_elephant_factor_without_print(
            self,
            contributions_count_by_org_mock,
            contributions_count_total_mock,
            init_notebook_mode_mock,
            go_offline_mock):
        """Test elephant factor method without plotly print mode active.
        """

        expected_data = {
            'organization': ['Lled', 'Marble', 'Nanosoft'],
            'contributions': [175, 165, 30]
        }

        expected_df = pandas.DataFrame(
            expected_data,
            columns=['organization', 'contributions'])
        expected_df['contributions'].iplot = MagicMock(return_value='test')

        contributions_count_total_mock.return_value = 175 + 165 + 30

        contributions_count_by_org_mock.return_value = expected_df

        # Test with start and end dates
        #

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = fm.elephant_factor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            print_dist=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=True)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=True)
        go_offline_mock.assert_not_called()
        init_notebook_mode_mock.assert_not_called()
        expected_df['contributions'].iplot.assert_not_called()

        self.assertEqual(result, 2)

        # Test with start date only
        #

        result = fm.elephant_factor(
            DataSource.ALL,
            start_date=start_date,
            print_dist=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None,
            exclude_unknown=True)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None,
            exclude_unknown=True)
        go_offline_mock.assert_not_called()
        init_notebook_mode_mock.assert_not_called()
        expected_df['contributions'].iplot.assert_not_called()

        self.assertEqual(result, 2)

        # Test with start date and exclude unknown
        #

        result = fm.elephant_factor(
            DataSource.GIT,
            start_date=start_date,
            exclude_unknown=False,
            print_dist=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None,
            exclude_unknown=False)
        go_offline_mock.assert_not_called()
        init_notebook_mode_mock.assert_not_called()
        expected_df['contributions'].iplot.assert_not_called()

        self.assertEqual(result, 2)

        # Test with both dates and exclude unknown
        #

        result = fm.elephant_factor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False,
            print_dist=False)

        contributions_count_total_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)
        contributions_count_by_org_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)
        go_offline_mock.assert_not_called()
        init_notebook_mode_mock.assert_not_called()
        expected_df['contributions'].iplot.assert_not_called()

        self.assertEqual(result, 2)


if __name__ == '__main__':
    unittest.main()
