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

import certifi
import os
import pandas
import sys
import unittest

from elasticsearch import RequestsHttpConnection
from elasticsearch_dsl import Search
from pandas.testing import assert_frame_equal
from unittest import TestCase, mock
from unittest.mock import MagicMock

# Make sure we use our code and not any other could we have installed
sys.path.insert(0, '..')

import broomstick.data.es.common as esc

from broomstick.core import DataSource


class TestESCommon(TestCase):

    def setUp(self):
        self.__data_dir = os.path.dirname(os.path.realpath(__file__))

    @mock.patch('broomstick.data.es.common.Elasticsearch',
                return_value='test_es_conn')
    def test_create_es_connection(self, es_mock):
        """Test create an es connection from a given .settings file.

        Uses a dummy configuration file to test the parameters are
        correctly used to create the connection.
        """

        config_file = os.path.join(self.__data_dir, 'data/settings.test')
        es_conn = esc.create_es_connection(config_file=config_file)

        url = "https://jane:doe@localhost:443/data"
        es_mock.assert_called_with([url],
                                   verify_certs=False,
                                   connection_class=RequestsHttpConnection,
                                   ca_cert=certifi.where(),
                                   scroll='300m',
                                   timeout=1000)

        self.assertEqual(es_conn, 'test_es_conn')

    def test_add_date_filter_min_date(self):
        """Test add filter calls with `start_date`.
        """

        s = Search()
        s.filter = MagicMock(return_value='test')

        min_date = '2018-01-01'
        result = esc.add_date_filter(s, start_date=min_date)

        s.filter.assert_called_with(
            'range',
            grimoire_creation_date={'gt': min_date})
        self.assertEqual(result, 'test')

    def test_add_date_filter_max_date(self):
        """Test add filter calls with `start_date` and `end_date`.
        """

        s = Search()
        s.filter = MagicMock(return_value='test')

        start_date = '2018-01-01'
        end_date = '2020-01-01'
        result = esc.add_date_filter(s, start_date=start_date,
                                     end_date=end_date)

        s.filter.assert_called_with(
            'range',
            grimoire_creation_date={"gt": start_date, "lte": end_date})
        self.assertEqual(result, 'test')

    @mock.patch('broomstick.data.es.common.Search',
                return_value='test_search')
    @mock.patch('broomstick.data.es.common.add_date_filter',
                return_value='search_with_filters')
    @mock.patch('broomstick.data.es.common.__es_conn',
                return_value='bar')
    def test_create_search_with_dates(self,
                                      es_conn_mock,
                                      add_date_filter_mock,
                                      search_mock):
        """Test create search function with min and max dates.
        """

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        s = esc.create_search(DataSource.GIT, start_date, end_date)

        search_mock.assert_called_with(
            using=es_conn_mock,
            index=esc.DS_INDEX[DataSource.GIT])
        add_date_filter_mock.assert_called_with('test_search',
                                                start_date, end_date)
        self.assertEqual(s, 'search_with_filters')

    @mock.patch('broomstick.data.es.common.Search',
                return_value='test_search')
    @mock.patch('broomstick.data.es.common.add_date_filter',
                return_value='search_with_filters')
    @mock.patch('broomstick.data.es.common.__es_conn',
                return_value='bar')
    def test_create_search_with_min_date(self,
                                         es_conn_mock,
                                         add_date_filter_mock,
                                         search_mock):
        """Test create search function with start date only.
        """

        start_date = '2018-01-01'

        s = esc.create_search(DataSource.GIT, start_date)
        search_mock.assert_called_with(
            using=es_conn_mock,
            index=esc.DS_INDEX[DataSource.GIT])
        add_date_filter_mock.assert_called_with(
            'test_search',
            start_date,
            None)
        self.assertEqual(s, 'search_with_filters')

    def test_exclude_org(self):
        """Test add organization name exclusion filter.
        """

        s = Search()
        s.exclude = MagicMock(return_value='test')

        result = esc.exclude_org(s, esc.UNKNOWN_ORG_NAME)

        s.exclude.assert_called_with(
            'term',
            author_org_name=esc.UNKNOWN_ORG_NAME)
        self.assertEqual(result, 'test')

    def test_ignore_bots(self):
        """Test add bot exclusion filter.
        """

        s = Search()
        s.exclude = MagicMock(return_value='test')

        result = esc.ignore_bots(s)

        s.exclude.assert_called_with(
            'bool',
            author_bot=True)
        self.assertEqual(result, 'test')


    def test_filter_org(self):
        """Tests add organization name inclusion filter.
        """

        s = Search()
        s.filter = MagicMock(return_value='test')

        result = esc.filter_org(s, esc.UNKNOWN_ORG_NAME)

        s.filter.assert_called_with(
            'term',
            author_org_name=esc.UNKNOWN_ORG_NAME)
        self.assertEqual(result, 'test')

    @mock.patch('broomstick.data.es.common.create_search')
    @mock.patch('broomstick.data.es.common.exclude_org')
    def test_contributions_count_total(self,
                                       exclude_org_mock,
                                       create_search_mock):
        """Test count total contributions method.
        """
        response = {
            'aggregations': {
                'total_contribs': {
                    'value': 125
                }
            }
        }

        # Create a mocked Search
        s, r = self.__create_mocked_search(response, create_search_mock)

        # Mock `exclude_org` to return our mocked `Search` object
        exclude_org_mock.return_value = s

        # Test with start and end dates
        #

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = esc.contributions_count_total(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)
        exclude_org_mock.assert_called_with(
            s=s,
            org_name=esc.UNKNOWN_ORG_NAME
        )
        s.aggs.metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000
        )
        self.assertEqual(result, 125)

        # Test with start date only
        #

        # Change response to make sure everything executes again
        response = {
            'aggregations': {
                'total_contribs': {
                    'value': 3012
                }
            }
        }
        r.to_dict = MagicMock(return_value=response)

        result = esc.contributions_count_total(
            DataSource.GIT,
            start_date=start_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None)
        exclude_org_mock.assert_called_with(
            s=s,
            org_name=esc.UNKNOWN_ORG_NAME
        )
        s.aggs.metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000
        )
        self.assertEqual(result, 3012)

        # Test excludincluding Unknown and both dates
        #

        # Change response to make sure everything executes again
        response = {
            'aggregations': {
                'total_contribs': {
                    'value': 101010
                }
            }
        }
        r.to_dict = MagicMock(return_value=response)

        # Reset the `exclude_org_mock` call number
        exclude_org_mock.reset_mock()

        result = esc.contributions_count_total(
            DataSource.ALL,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        create_search_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=end_date)
        exclude_org_mock.assert_not_called()
        s.aggs.metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='painless_unique_id',
            precision_threshold=40000
        )
        self.assertEqual(result, 101010)

        # Test excludincluding Unknown and start date only
        #

        # Change response to make sure everything executes again
        response = {
            'aggregations': {
                'total_contribs': {
                    'value': 200200
                }
            }
        }
        r.to_dict = MagicMock(return_value=response)

        # Reset the `exclude_org_mock` call number
        exclude_org_mock.reset_mock()

        result = esc.contributions_count_total(
            DataSource.GIT,
            start_date=start_date,
            exclude_unknown=False)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None)
        exclude_org_mock.assert_not_called()
        s.aggs.metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000
        )
        self.assertEqual(result, 200200)

    @mock.patch('broomstick.data.es.common.create_search')
    @mock.patch('broomstick.data.es.common.filter_org')
    def test_contributions_count_unknown(self,
                                         filter_org_mock,
                                         create_search_mock):
        """Test count contributions sent by people affiliated to Unknown method.
        """
        response = {
            'aggregations': {
                'unknown_contribs': {
                    'value': 1555
                }
            }
        }

        # Create a mocked Search
        s, r = self.__create_mocked_search(response, create_search_mock)

        # Mock `exclude_org` to return our mocked `Search` object
        filter_org_mock.return_value = s

        # Test with start and end dates
        #

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = esc.contributions_count_unknown(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)
        filter_org_mock.assert_called_with(
            s=s,
            org_name=esc.UNKNOWN_ORG_NAME
        )
        s.aggs.metric.assert_called_with(
            'unknown_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000
        )
        self.assertEqual(result, 1555)

        # Test with start date only
        #

        # Change response to make sure everything executes again
        response = {
            'aggregations': {
                'unknown_contribs': {
                    'value': 6543
                }
            }
        }
        r.to_dict = MagicMock(return_value=response)

        result = esc.contributions_count_unknown(
            DataSource.ALL,
            start_date=start_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None)
        filter_org_mock.assert_called_with(
            s=s,
            org_name=esc.UNKNOWN_ORG_NAME
        )
        s.aggs.metric.assert_called_with(
            'unknown_contribs',
            'cardinality',
            field='painless_unique_id',
            precision_threshold=40000
        )
        self.assertEqual(result, 6543)

    @mock.patch('broomstick.data.es.common.create_search')
    @mock.patch('broomstick.data.es.common.exclude_org')
    def test_contributions_count_by_org(self,
                                        exclude_org_mock,
                                        create_search_mock):
        """Test count total contributions method.
        """
        response = {
            'aggregations': {
                'organizations': {
                    'doc_count_error_upper_bound': 0,
                    'sum_other_doc_count': 0,
                    'buckets': [
                        {
                            'key': 'Lled',
                            'doc_count': 213,
                            'total_contribs': {
                                'value': 179
                            }
                        },
                        {
                            'key': 'Marble',
                            'doc_count': 130,
                            'total_contribs': {
                                'value': 125
                            }
                        },
                        {
                            'key': 'Nanosoft',
                            'doc_count': 33,
                            'total_contribs': {
                                'value': 30
                            }
                        }
                    ]
                }
            }
        }

        expected_data = {
            'organization': ['Lled', 'Marble', 'Nanosoft'],
            'contributions': [179, 125, 30]
        }

        expected_df = pandas.DataFrame(
            expected_data,
            columns=['organization', 'contributions'])

        # Create a mocked Search
        s, r = self.__create_mocked_search(response, create_search_mock)

        # Mock `exclude_org` to return our mocked `Search` object
        exclude_org_mock.return_value = s

        # Test with start and end dates
        #

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = esc.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)
        exclude_org_mock.assert_called_with(
            s=s,
            org_name=esc.UNKNOWN_ORG_NAME)

        s.aggs.bucket.assert_called_with(
            'organizations',
            'terms',
            field='author_org_name',
            order={'total_contribs': 'desc'},
            size=1000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

        # Test with start date only
        #

        result = esc.contributions_count_by_org(
            DataSource.ALL,
            start_date=start_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None)
        exclude_org_mock.assert_called_with(
            s=s,
            org_name=esc.UNKNOWN_ORG_NAME
        )

        s.aggs.bucket.assert_called_with(
            'organizations',
            'terms',
            field='author_org_name',
            order={'total_contribs': 'desc'},
            size=1000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='painless_unique_id',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

        # Test excludincluding Unknown and both dates
        #

        # Reset the `exclude_org_mock` call number
        exclude_org_mock.reset_mock()

        result = esc.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_unknown=False)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)
        exclude_org_mock.assert_not_called()

        s.aggs.bucket.assert_called_with(
            'organizations',
            'terms',
            field='author_org_name',
            order={'total_contribs': 'desc'},
            size=1000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

        # Test excludincluding Unknown and start date only
        #

        # Reset the `exclude_org_mock` call number
        exclude_org_mock.reset_mock()

        result = esc.contributions_count_by_org(
            DataSource.GIT,
            start_date=start_date,
            exclude_unknown=False)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None)
        exclude_org_mock.assert_not_called()

        s.aggs.bucket.assert_called_with(
            'organizations',
            'terms',
            field='author_org_name',
            order={'total_contribs': 'desc'},
            size=1000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

    @mock.patch('broomstick.data.es.common.create_search')
    @mock.patch('broomstick.data.es.common.ignore_bots')
    def test_contributions_count_by_contributor(self,
                                        ignore_bots_mock,
                                        create_search_mock):
        """Test count total contributions by contributor method.
        """
        response = {
            'aggregations': {
                'contributors': {
                    'doc_count_error_upper_bound': 0,
                    'sum_other_doc_count': 0,
                    'buckets': [
                        {
                            'key': 'Anne',
                            'doc_count': 213,
                            'total_contribs': {
                                'value': 179
                            }
                        },
                        {
                            'key': 'Bob',
                            'doc_count': 130,
                            'total_contribs': {
                                'value': 125
                            }
                        },
                        {
                            'key': 'Carl',
                            'doc_count': 33,
                            'total_contribs': {
                                'value': 30
                            }
                        }
                    ]
                }
            }
        }

        expected_data = {
            'contributor': ['Anne', 'Bob', 'Carl'],
            'contributions': [179, 125, 30]
        }

        expected_df = pandas.DataFrame(
            expected_data,
            columns=['contributor', 'contributions'])

        # Create a mocked Search
        s, r = self.__create_mocked_search(response, create_search_mock)

        # Mock `ignore_bots` to return our mocked `Search` object
        ignore_bots_mock.return_value = s

        # Test with start and end dates
        #

        start_date = '2018-01-01'
        end_date = '2020-01-01'

        result = esc.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)
        ignore_bots_mock.assert_called_with(
            s=s)

        s.aggs.bucket.assert_called_with(
            'contributors',
            'terms',
            field='author_name',
            order={'total_contribs': 'desc'},
            size=10000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

        # Test with start date only
        #

        result = esc.contributions_count_by_contributor(
            DataSource.ALL,
            start_date=start_date)

        create_search_mock.assert_called_with(
            data_source=DataSource.ALL,
            start_date=start_date,
            end_date=None)
        ignore_bots_mock.assert_called_with(
            s=s)

        s.aggs.bucket.assert_called_with(
            'contributors',
            'terms',
            field='author_name',
            order={'total_contribs': 'desc'},
            size=10000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='painless_unique_id',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

        # Test (not) ignoring bots and both dates
        #

        # Reset the `ignore_bots_mock` call number
        ignore_bots_mock.reset_mock()

        result = esc.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date,
            end_date=end_date,
            exclude_bots=False)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=end_date)
        ignore_bots_mock.assert_not_called()

        s.aggs.bucket.assert_called_with(
            'contributors',
            'terms',
            field='author_name',
            order={'total_contribs': 'desc'},
            size=10000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

        # Test (not) ignoring bots and start date only
        #

        # Reset the `ignore_bots_mock` call number
        ignore_bots_mock.reset_mock()

        result = esc.contributions_count_by_contributor(
            DataSource.GIT,
            start_date=start_date,
            exclude_bots=False)

        create_search_mock.assert_called_with(
            data_source=DataSource.GIT,
            start_date=start_date,
            end_date=None)
        ignore_bots_mock.assert_not_called()

        s.aggs.bucket.assert_called_with(
            'contributors',
            'terms',
            field='author_name',
            order={'total_contribs': 'desc'},
            size=10000)
        s.aggs.bucket().metric.assert_called_with(
            'total_contribs',
            'cardinality',
            field='hash',
            precision_threshold=40000)

        assert_frame_equal(result, expected_df)

    def __create_mocked_search(self, response, create_search_mock):
        # Create a mocked Search
        s = MagicMock()
        s.__getitem__ = MagicMock(return_value=s)
        # Create a mocked `Response` for the `Search` object
        r = MagicMock()
        r.to_dict = MagicMock(return_value=response)
        s.execute = MagicMock(return_value=r)

        # Mock `create_search` to return our mocked `Search` object
        create_search_mock.return_value = s
        return s, r


if __name__ == '__main__':
    unittest.main()
