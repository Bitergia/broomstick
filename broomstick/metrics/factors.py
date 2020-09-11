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

import cufflinks

from plotly.offline import init_notebook_mode


import broomstick.metrics.general as gm


def elephant_factor(data_source,
                    start_date,
                    end_date=None,
                    exclude_unknown=True,
                    print_dist=True):
    """Computes the Elephant Factor.

    :param data_source: `broomstick.core.DataSource`
    :param start_date: date from which we want to start counting contributions
        (exclusive).
    :param end_date: date until we want to counts contributions to (inclusive).
        `None` by default, means count everything from `start_date`.
    :param exclude_unknown: whether or not to exclude contributions sent by
        people affiliated to 'Unknown' organization.
    :returns: the number of organizations sending up to the 50% of
        contributions.
    """

    total_contributions = gm.contributions_count_total(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        exclude_unknown=exclude_unknown)

    org_contributions_df = gm.contributions_count_by_org(
        data_source=data_source,
        start_date=start_date,
        end_date=end_date,
        exclude_unknown=exclude_unknown)

    threshold = total_contributions * 0.5

    threshold_org = org_contributions_df.at[
        org_contributions_df['contributions']
        .cumsum().sub(threshold).ge(0).idxmax(), 'organization']

    if print_dist:

        # Use plotly + cufflinks in offline mode
        cufflinks.go_offline(connected=True)
        init_notebook_mode(connected=True)

        org_contributions_df['contributions'].iplot(
            kind='hist',
            xTitle='contributions',
            yTitle='Organizations',
            title='contributions Distribution')

    return org_contributions_df.index[
        org_contributions_df['organization'] == threshold_org][0] + 1
