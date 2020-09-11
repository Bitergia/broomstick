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
import configparser
import pandas
import urllib3


from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search

from broomstick.core import DataSource

# Disable urlib3 warnings
urllib3.disable_warnings()


# Data source to index name correpondence
DS_INDEX = {
    DataSource.ALL: 'all_enriched',
    DataSource.GIT: 'git'
}

DS_ID_FIELD = {
    DataSource.ALL: 'painless_unique_id',
    DataSource.GIT: 'hash'
}

UNKNOWN_ORG_NAME = 'Unknown'

# Connection to ElasticSearch, all functions should use the same
__es_conn = None


def create_es_connection(config_file='.settings'):
    """Creates and returns a new ElasticSearch connection.

    Creates a new connection to the ElasticSearch server if it doesn't exist
    yet.

    :param config_file: configuration file with the following format:

        [ElasticSearch]
        user=user
        password=pass
        host=localhost
        port=443
        path=path

    :returns: the connection to the ES server.
    """

    global __es_conn

    if not __es_conn:
        parser = configparser.ConfigParser()
        parser.read(config_file)

        section = parser['ElasticSearch']
        user = section['user']
        password = section['password']
        host = section['host']
        port = section['port']
        path = section['path']

        connection = "https://" + user + ":" + password + "@" + host + ":" \
                     + port + "/" + path

        __es_conn = Elasticsearch([connection],
                                  verify_certs=False,
                                  connection_class=RequestsHttpConnection,
                                  ca_cert=certifi.where(),
                                  scroll='300m',
                                  timeout=1000)

    return __es_conn


def create_search(data_source, start_date, end_date=None):
    """ Creates and returns a new ES Search object.

    The returned search is configured against the
    given data source, and with a date filter between start_date
    and end_date (optional).

    If the ES connection doesn't exist, it tries to create a new
    one using the default config file path: `.settings`.

    :param data_source: target index as `broomstick.data.general.DataSource`.
    :param start_date: date range start (exclusive).
    :param end_date: date range end (inclusive). If `end_date` is not provided,
        adds a filter to retrieve documents from `start_date`.
    :returns: the search object configured according to the params.
    """

    if not __es_conn:
        create_es_connection()

    s = Search(using=__es_conn, index=DS_INDEX[data_source])

    # Add bot, merges and date filtering.
    s = add_date_filter(s, start_date, end_date)

    return s


def add_date_filter(s, start_date, end_date=None):
    """Adds a filter to retrieve documents created between start and end dates.

    :param start_date: date range start (exclusive).
    :param end_date: date range end (inclusive). If `end_date` is not provided,
        adds a filter to retrieve documents from `start_date`.
    :returns: the search with the desired filter set.
    """

    if start_date and end_date:
        s = s.filter(
            'range',
            grimoire_creation_date={"gt": start_date, "lte": end_date})

    elif start_date:
        s = s.filter('range', grimoire_creation_date={'gt': start_date})

    return s


def exclude_org(s, org_name):
    """Adds a filter for excluding authors affiliated to the given org.

    :param s: the search we want to update.
    :param org_name: organization name to exclude.
    :returns: the search with the exclusion filter set.
    """
    return s.exclude('term', author_org_name=org_name)


def filter_org(s, org_name):
    """Adds a filter for retrieving only authors affiliated to the given org.

    :param org_name: organization name to filter in.
    :returns: the search with the inclusion filter set.
    """
    return s.filter('term', author_org_name=org_name)


def contributions_count_total(data_source,
                              start_date,
                              end_date=None,
                              exclude_unknown=True):
    """Get total number of contributions.

    :param data_source: target `broomstick.data.general.DataSource`.
    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :param exclude_unknown: whether or not to exclude contributions sent by
        people affiliated to 'Unknown' organization.
    :returns: the number of contributions sent to the specified data source.
    """

    s = create_search(data_source=data_source,
                      start_date=start_date,
                      end_date=end_date)

    if exclude_unknown:
        s = exclude_org(s=s, org_name=UNKNOWN_ORG_NAME)

    s.aggs.metric('total_contribs',
                  'cardinality',
                  field=DS_ID_FIELD[data_source],
                  precision_threshold=40000)
    s = s[0:0]

    return s.execute().to_dict()['aggregations']['total_contribs']['value']


def contributions_count_unknown(data_source,
                                start_date,
                                end_date=None):
    """ Get total number of contributions performed by Unknown

    :param data_source: `broomstick.core.DataSource`
    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :returns: the number of contributions sent by people affiliated to
        'Unknown' to the specified data source.
    """
    s = create_search(data_source=data_source,
                      start_date=start_date,
                      end_date=end_date)

    s = filter_org(s=s, org_name=UNKNOWN_ORG_NAME)

    s.aggs.metric('unknown_contribs',
                  'cardinality',
                  field=DS_ID_FIELD[data_source],
                  precision_threshold=40000)
    s = s[0:0]

    return s.execute().to_dict()['aggregations']['unknown_contribs']['value']


def contributions_count_by_org(data_source,
                               start_date,
                               end_date=None,
                               exclude_unknown=True):
    """ Gets number of contributions of each organization.

    :param data_source: `broomstick.core.DataSource`
    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :param exclude_unknown: whether or not to exclude contributions sent by
        people affiliated to 'Unknown' organization.
    :returns: a Pandas DataFrame with two columns:
        - Organization name.
        - The number of contributions sent by that organization to the
          specified data source during the given dates.
    """

    s = create_search(data_source=data_source,
                      start_date=start_date,
                      end_date=end_date)

    if exclude_unknown:
        s = exclude_org(s=s, org_name=UNKNOWN_ORG_NAME)

    s.aggs.bucket('organizations',
                  'terms',
                  field='author_org_name',
                  order={'total_contribs': 'desc'},
                  size=1000)\
        .metric('total_contribs',
                'cardinality',
                field=DS_ID_FIELD[data_source],
                precision_threshold=40000)
    s = s[0:0]

    buckets = s.execute().to_dict()['aggregations']['organizations']['buckets']

    contribs_by_org_df = pandas.json_normalize(buckets)

    # remove `doc_count` column
    contribs_by_org_df = contribs_by_org_df.drop(['doc_count'], axis=1)

    contribs_by_org_df.rename(
        columns={
            'key': 'organization',
            'total_contribs.value': 'contributions'},
        inplace=True)

    return contribs_by_org_df
