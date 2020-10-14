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

from broomstick.data.es import common as com


def contributions_count_total(data_source,
                              start_date,
                              end_date=None,
                              exclude_unknown=True):
    """ Get total number of contributions

    :param data_source: `broomstick.core.DataSource`
    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :param exclude_unknown: whether or not to exclude contributions sent by
        people affiliated to 'Unknown' organization.
    :returns: the number of contributions sent to the specified data source.
    """
    return com.contributions_count_total(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        exclude_unknown=exclude_unknown)


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
    return com.contributions_count_unknown(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date)


def contributions_unknown_percentage(data_source,
                                     start_date,
                                     end_date=None):
    """Compute the percentage of contributions sent by people affiliated to
        'Unknown'.

    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :returns: the percentage of contributions sent by people affiliated to
        'Unknown' to the specified data source.
    """
    total_contributions = contributions_count_total(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        exclude_unknown=False)
    unknown_contributions = contributions_count_unknown(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date)

    return (unknown_contributions / total_contributions) * 100


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
    :returns: the number of contributions sent to the specified data source.
    """

    return com.contributions_count_by_org(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        exclude_unknown=exclude_unknown)


def contributions_count_by_contributor(data_source,
                                       start_date,
                                       end_date=None,
                                       exclude_bots=True):
    """ Gets number of contributions of each contributor.

    :param data_source: `broomstick.core.DataSource`
    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :param exclude_bots: whether or not to exclude contributions by bots
    :returns: the number of contributions sent to the specified data source.
    """

    return com.contributions_count_by_contributor(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        exclude_bots=exclude_bots)

