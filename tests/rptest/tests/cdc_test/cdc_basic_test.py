# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool


class CDCBasicTest(RedpandaTest):
    """
    Basic integration tests for CDC functionality
    """

    def __init__(self, test_context):
        super(CDCBasicTest, self).__init__(test_context=test_context)

    @cluster(num_nodes=3)
    def test_cdc_connector_lifecycle(self):
        """
        Test creating, listing, and deleting CDC connectors
        """
        # TODO: Implement test that:
        # 1. Creates a CDC connector via Admin API
        # 2. Lists connectors and verifies it appears
        # 3. Gets connector status
        # 4. Deletes connector
        # 5. Verifies it's removed
        pass

    @cluster(num_nodes=3)
    def test_postgres_cdc_snapshot(self):
        """
        Test PostgreSQL CDC snapshot mode
        """
        # TODO: Implement test that:
        # 1. Starts PostgreSQL container
        # 2. Creates test table and inserts data
        # 3. Creates CDC connector in snapshot mode
        # 4. Verifies all rows appear as change events
        pass

    @cluster(num_nodes=3)
    def test_mysql_cdc_incremental(self):
        """
        Test MySQL CDC incremental mode
        """
        # TODO: Implement test that:
        # 1. Starts MySQL container
        # 2. Creates CDC connector in incremental mode
        # 3. Performs INSERT, UPDATE, DELETE operations
        # 4. Verifies change events are captured
        pass

    @cluster(num_nodes=3)
    def test_cdc_transforms(self):
        """
        Test CDC transforms
        """
        # TODO: Implement test that:
        # 1. Creates CDC connector with transforms
        # 2. Verifies transforms are applied to events
        pass

    @cluster(num_nodes=3)
    def test_cdc_checkpoint_recovery(self):
        """
        Test CDC checkpoint and recovery
        """
        # TODO: Implement test that:
        # 1. Creates CDC connector
        # 2. Processes some events
        # 3. Simulates failure
        # 4. Verifies connector recovers from checkpoint
        pass
