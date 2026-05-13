# -*- coding: utf8 -*-

import unittest
import time
import sys

from tablestore import *
from tablestore.error import *
from tests.lib import test_config

PK1_NAME = "PkString"
PK2_NAME = "PkInt"
ATTR1_NAME = "Attr1"


def _generate_global_table_name_prefix():
    python_version = '%s_%s_%s' % (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    return "py_" + python_version + "_" + time.strftime("%Y%m%d%H%M%S")


def retry_with_timeout(operation, timeout_seconds, interval_seconds):
    """Retry an operation until it succeeds or times out."""
    deadline = time.time() + timeout_seconds
    last_error = None

    result = None
    try:
        result = operation()
        return result
    except Exception as first_error:
        last_error = first_error

    while time.time() < deadline:
        time.sleep(interval_seconds)
        try:
            result = operation()
            return result
        except Exception as error:
            last_error = error

    raise Exception("operation timed out after %ds, last error: %s" % (timeout_seconds, str(last_error)))


def waiting_reconf_completed(client, global_table_id, global_table_name):
    """Wait until the global table status is no longer RECONF."""
    def check_status():
        try:
            resp = client.describe_global_table(
                DescribeGlobalTableRequest(
                    global_table_name=global_table_name,
                    global_table_id=global_table_id,
                )
            )
        except OTSServiceError as error:
            if "not a global table" in str(error.message).lower():
                print("[waiting_reconf_completed] (expected) table is not a global table yet, reconf completed")
                return None
            raise error

        if resp.status == GlobalTableStatus.RECONF:
            raise Exception("still in reconf")
        return None

    retry_with_timeout(check_status, 12 * 60, 3)


def prepare_sample_base_table(client, table_name):
    """Create a base table for global table testing."""
    schema = [(PK1_NAME, 'STRING'), (PK2_NAME, 'INTEGER')]
    defined_columns = [(ATTR1_NAME, 'STRING')]
    table_meta = TableMeta(table_name, schema, defined_columns)
    table_options = TableOptions(time_to_live=-1, max_version=1, max_time_deviation=2147483647, update_full_row=True)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)


def prepare_simple_global_table(client, region, instance_name, table_name):
    """Create a simple global table without placements."""
    request = CreateGlobalTableRequest(
        base_table=BaseTable(
            region_id=region,
            instance_name=instance_name,
            table_name=table_name,
        ),
        sync_mode=SyncMode.ROW,
        serve_mode=ServeMode.PRIMARY_SECONDARY,
    )
    response = client.create_global_table(request)
    return response.global_table_id


def prepare_global_table_with_placement(client, region, instance_name, table_name,
                                        delete_placement_if_exist, waiting_after_delete,
                                        placement_client):
    """Create a global table with a placement in the secondary region."""
    request = CreateGlobalTableRequest(
        base_table=BaseTable(
            region_id=region,
            instance_name=instance_name,
            table_name=table_name,
        ),
        sync_mode=SyncMode.ROW,
        serve_mode=ServeMode.PRIMARY_SECONDARY,
        placements=[
            Placement(
                region_id=test_config.OTS_GLOBAL_TABLE_PLACEMENT_REGION,
                instance_name=test_config.OTS_GLOBAL_TABLE_PLACEMENT_INSTANCE,
                writable=False,
            ),
        ],
    )

    try:
        response = client.create_global_table(request)
        return response.global_table_id
    except OTSServiceError as error:
        if "already exist" not in str(error.message):
            raise error
        if not delete_placement_if_exist:
            raise error

        try:
            placement_client.delete_table(table_name)
        except Exception:
            pass
        time.sleep(waiting_after_delete)

        response = client.create_global_table(request)
        return response.global_table_id


@unittest.skipIf(
    not test_config.OTS_GLOBAL_TABLE_PLACEMENT_ENDPOINT,
    "Global Table placement endpoint not configured"
)
class GlobalTableTest(unittest.TestCase):
    """Global Table API integration tests, migrated from Go SDK."""

    global_table_name_prefix = None
    default_global_table_name = None
    region_a_client = None
    region_b_client = None

    @classmethod
    def setUpClass(cls):
        cls.global_table_name_prefix = _generate_global_table_name_prefix()
        cls.default_global_table_name = cls.global_table_name_prefix + "_default_globaltable"

        cls.region_a_client = OTSClient(
            test_config.OTS_ENDPOINT,
            test_config.OTS_ACCESS_KEY_ID,
            test_config.OTS_ACCESS_KEY_SECRET,
            test_config.OTS_INSTANCE,
            region=test_config.OTS_REGION,
        )

        cls.region_b_client = OTSClient(
            test_config.OTS_GLOBAL_TABLE_PLACEMENT_ENDPOINT,
            test_config.OTS_ACCESS_KEY_ID,
            test_config.OTS_ACCESS_KEY_SECRET,
            test_config.OTS_GLOBAL_TABLE_PLACEMENT_INSTANCE,
            region=test_config.OTS_GLOBAL_TABLE_PLACEMENT_REGION,
        )

        prepare_sample_base_table(cls.region_a_client, cls.default_global_table_name)

    @classmethod
    def tearDownClass(cls):
        try:
            retry_with_timeout(
                lambda: cls._check_global_table_id_cleared(cls),
                3 * 60, 5,
            )
        except Exception as error:
            print("[tearDownClass] wait for global_table_id cleared failed: %s" % str(error))

        try:
            cls.region_a_client.delete_table(cls.default_global_table_name)
        except Exception as error:
            print("[tearDownClass] clear base table failed: %s" % str(error))
        else:
            print("[tearDownClass] clear base table success")

    def _check_global_table_id_cleared(self):
        table_resp = self.region_a_client.describe_table(self.default_global_table_name)
        table_meta = table_resp.table_meta
        if table_meta is None:
            return None
        if hasattr(table_meta, 'global_table_id') and table_meta.global_table_id:
            raise Exception("GlobalTableId is not empty")
        return None

    def tearDown(self):
        try:
            retry_with_timeout(
                lambda: self._unbind_and_delete_placement_table(self.default_global_table_name),
                6 * 60, 5,
            )
        except Exception as error:
            print("[tearDown] clear placement table failed after retry: %s" % str(error))
        else:
            print("[tearDown] clear placement table success")

    def _unbind_and_delete_placement_table(self, table_name):
        """Unbind all placements and delete placement tables."""
        describe_req = DescribeGlobalTableRequest(
            global_table_name=table_name,
            phy_table=PhyTable(
                region_id=test_config.OTS_REGION,
                instance_name=test_config.OTS_INSTANCE,
                table_name=table_name,
            ),
        )
        describe_global_id_resp = self.region_a_client.describe_global_table(describe_req)

        desc_resp = self.region_a_client.describe_global_table(
            DescribeGlobalTableRequest(
                global_table_id=describe_global_id_resp.global_table_id,
                global_table_name=table_name,
            )
        )

        remove_placements = []
        remove_primary = None
        for phy_table in desc_resp.phy_tables:
            if phy_table.role == "primary":
                remove_primary = Removal(
                    region_id=phy_table.region_id,
                    instance_name=phy_table.instance_name,
                )
            else:
                remove_placements.append(Removal(
                    region_id=phy_table.region_id,
                    instance_name=phy_table.instance_name,
                ))

        if remove_placements:
            unbind_req = UnbindGlobalTableRequest(
                global_table_id=desc_resp.global_table_id,
                global_table_name=table_name,
                removals=remove_placements,
            )
            self.region_a_client.unbind_global_table(unbind_req)
            waiting_reconf_completed(
                self.region_a_client,
                desc_resp.global_table_id,
                self.default_global_table_name,
            )

        if remove_primary is not None:
            unbind_req = UnbindGlobalTableRequest(
                global_table_id=desc_resp.global_table_id,
                global_table_name=table_name,
                removals=[remove_primary],
            )
            self.region_a_client.unbind_global_table(unbind_req)
            waiting_reconf_completed(
                self.region_a_client,
                desc_resp.global_table_id,
                self.default_global_table_name,
            )

        for phy_table in desc_resp.phy_tables:
            if (phy_table.region_id == test_config.OTS_GLOBAL_TABLE_PLACEMENT_REGION and
                    phy_table.instance_name == test_config.OTS_GLOBAL_TABLE_PLACEMENT_INSTANCE):
                try:
                    self.region_b_client.delete_table(phy_table.table_name)
                    print("deleted placement table: %s, region: %s" % (phy_table.table_name, phy_table.region_id))
                except Exception:
                    pass

    def _write_data_to_primary_table(self, pk1, pk2, attr_value1):
        """Write a row to the primary table."""
        primary_key = [(PK1_NAME, pk1), (PK2_NAME, pk2)]
        attribute_columns = [(ATTR1_NAME, attr_value1)]
        row = Row(primary_key, attribute_columns)
        condition = Condition(RowExistenceExpectation.IGNORE)
        self.region_a_client.put_row(
            self.default_global_table_name, row, condition
        )

    def _verify_data_in_placement_table(self, pk1, pk2, attr_value1):
        """Verify data exists in the placement table with retry."""
        primary_key = [(PK1_NAME, pk1), (PK2_NAME, pk2)]

        def check_data():
            consumed, return_row, next_token = self.region_b_client.get_row(
                self.default_global_table_name, primary_key, max_version=1,
            )
            if return_row is None:
                raise Exception("no data found in placement table")

            found = False
            for col_name, col_value, col_ts in return_row.attribute_columns:
                if col_name == ATTR1_NAME and col_value == attr_value1:
                    found = True
                    break

            if not found:
                raise Exception("data content mismatch in placement table")

        retry_with_timeout(check_data, 10 * 60, 3)

    def _verify_data_not_in_placement_table(self, pk1, pk2, attr_value1):
        """Verify data does NOT exist in the placement table for 1 minute."""
        primary_key = [(PK1_NAME, pk1), (PK2_NAME, pk2)]
        start_time = time.time()
        timeout = 60

        while time.time() - start_time < timeout:
            try:
                consumed, return_row, next_token = self.region_b_client.get_row(
                    self.default_global_table_name, primary_key, max_version=1,
                )
            except Exception:
                time.sleep(3)
                continue

            if return_row is not None:
                for col_name, col_value, col_ts in return_row.attribute_columns:
                    if col_name == ATTR1_NAME and col_value == attr_value1:
                        self.fail("Data should not be readable from placement table after unbinding")
                        return

            time.sleep(3)

        print("verified that data is not accessible from placement table for %ds" % timeout)

    def test_create_global_table(self):
        """Test creating a global table."""
        create_req = CreateGlobalTableRequest(
            base_table=BaseTable(
                region_id=test_config.OTS_REGION,
                instance_name=test_config.OTS_INSTANCE,
                table_name=self.default_global_table_name,
            ),
            sync_mode=SyncMode.ROW,
            serve_mode=ServeMode.PRIMARY_SECONDARY,
        )

        response = self.region_a_client.create_global_table(create_req)
        print("[test_create_global_table] request_id: %s, global_table_id: %s" % (response.request_id, response.global_table_id))
        self.assertIsNotNone(response.request_id)
        self.assertTrue(len(response.request_id) > 0)
        self.assertIsNotNone(response.global_table_id)
        self.assertTrue(len(response.global_table_id) > 0)

        waiting_reconf_completed(
            self.region_a_client,
            response.global_table_id,
            self.default_global_table_name,
        )

    def test_bind_global_table(self):
        """Test binding a placement to a global table and verifying data sync."""
        global_table_id = prepare_simple_global_table(
            self.region_a_client,
            test_config.OTS_REGION,
            test_config.OTS_INSTANCE,
            self.default_global_table_name,
        )

        bind_req = BindGlobalTableRequest(
            global_table_id=global_table_id,
            global_table_name=self.default_global_table_name,
            placements=[
                Placement(
                    region_id=test_config.OTS_GLOBAL_TABLE_PLACEMENT_REGION,
                    instance_name=test_config.OTS_GLOBAL_TABLE_PLACEMENT_INSTANCE,
                    writable=False,
                ),
            ],
        )
        response = self.region_a_client.bind_global_table(bind_req)
        self.assertIsNotNone(response.request_id)
        self.assertTrue(len(response.request_id) > 0)

        waiting_reconf_completed(
            self.region_a_client,
            global_table_id,
            self.default_global_table_name,
        )

        self._write_data_to_primary_table("test_key", 123, "test_value")
        self._verify_data_in_placement_table("test_key", 123, "test_value")

    def test_unbind_global_table(self):
        """Test unbinding a placement and verifying data no longer syncs."""
        global_table_id = prepare_global_table_with_placement(
            self.region_a_client,
            test_config.OTS_REGION,
            test_config.OTS_INSTANCE,
            self.default_global_table_name,
            True, 30,
            self.region_b_client,
        )

        waiting_reconf_completed(
            self.region_a_client,
            global_table_id,
            self.default_global_table_name,
        )

        unbind_req = UnbindGlobalTableRequest(
            global_table_id=global_table_id,
            global_table_name=self.default_global_table_name,
            removals=[
                Removal(
                    region_id=test_config.OTS_GLOBAL_TABLE_PLACEMENT_REGION,
                    instance_name=test_config.OTS_GLOBAL_TABLE_PLACEMENT_INSTANCE,
                ),
            ],
        )
        response = self.region_a_client.unbind_global_table(unbind_req)
        self.assertIsNotNone(response.request_id)
        self.assertTrue(len(response.request_id) > 0)

        waiting_reconf_completed(
            self.region_a_client,
            global_table_id,
            self.default_global_table_name,
        )

        self._write_data_to_primary_table("unbind_test_key", 456, "unbind_test_value")
        self._verify_data_not_in_placement_table("unbind_test_key", 456, "unbind_test_value")

        try:
            self.region_b_client.delete_table(self.default_global_table_name)
        except Exception:
            pass

    def test_describe_global_table(self):
        """Test describing a global table."""
        global_table_id = prepare_simple_global_table(
            self.region_a_client,
            test_config.OTS_REGION,
            test_config.OTS_INSTANCE,
            self.default_global_table_name,
        )

        describe_req = DescribeGlobalTableRequest(
            global_table_name=self.default_global_table_name,
            global_table_id=global_table_id,
        )
        response = self.region_a_client.describe_global_table(describe_req)
        self.assertIsNotNone(response.global_table_id)
        self.assertTrue(len(response.global_table_id) > 0)
        self.assertEqual(len(response.phy_tables), 1)

    def test_update_global_table(self):
        """Test updating a physical table in a global table."""
        global_table_id = prepare_global_table_with_placement(
            self.region_a_client,
            test_config.OTS_REGION,
            test_config.OTS_INSTANCE,
            self.default_global_table_name,
            True, 30,
            self.region_b_client,
        )

        waiting_reconf_completed(
            self.region_a_client,
            global_table_id,
            self.default_global_table_name,
        )

        update_req = UpdateGlobalTableRequest(
            global_table_id=global_table_id,
            global_table_name=self.default_global_table_name,
            phy_table=UpdatePhyTable(
                region_id=test_config.OTS_GLOBAL_TABLE_PLACEMENT_REGION,
                instance_name=test_config.OTS_GLOBAL_TABLE_PLACEMENT_INSTANCE,
                table_name=self.default_global_table_name,
                writable=False,
                primary_eligible=True,
            ),
        )
        response = self.region_a_client.update_global_table(update_req)
        self.assertIsNotNone(response.request_id)
        self.assertTrue(len(response.request_id) > 0)


class GlobalTableEncoderTest(unittest.TestCase):
    """Unit tests for Global Table encoder."""

    def setUp(self):
        from tablestore.encoder import OTSProtoBufferEncoder
        self.encoder = OTSProtoBufferEncoder('utf8')

    def test_encode_create_global_table(self):
        request = CreateGlobalTableRequest(
            base_table=BaseTable(
                region_id='cn-hangzhou',
                instance_name='test-instance',
                table_name='test-table',
            ),
            sync_mode=SyncMode.ROW,
            serve_mode=ServeMode.PRIMARY_SECONDARY,
            placements=[
                Placement(
                    region_id='cn-shanghai',
                    instance_name='test-instance-sh',
                    writable=False,
                ),
            ],
        )
        proto = self.encoder.encode_request('CreateGlobalTable', request)
        self.assertEqual(proto.baseTable.regionId, 'cn-hangzhou')
        self.assertEqual(proto.baseTable.instanceName, 'test-instance')
        self.assertEqual(proto.baseTable.tableName, 'test-table')
        self.assertEqual(len(proto.placements), 1)
        self.assertEqual(proto.placements[0].regionId, 'cn-shanghai')
        self.assertEqual(proto.placements[0].instanceName, 'test-instance-sh')
        self.assertFalse(proto.placements[0].writable)

    def test_encode_create_global_table_invalid_sync_mode(self):
        request = CreateGlobalTableRequest(
            base_table=BaseTable('cn-hangzhou', 'inst', 'tbl'),
            sync_mode='invalid',
            serve_mode=ServeMode.PRIMARY_SECONDARY,
        )
        with self.assertRaises(OTSClientError):
            self.encoder.encode_request('CreateGlobalTable', request)

    def test_encode_create_global_table_invalid_serve_mode(self):
        request = CreateGlobalTableRequest(
            base_table=BaseTable('cn-hangzhou', 'inst', 'tbl'),
            sync_mode=SyncMode.ROW,
            serve_mode='invalid',
        )
        with self.assertRaises(OTSClientError):
            self.encoder.encode_request('CreateGlobalTable', request)

    def test_encode_bind_global_table(self):
        request = BindGlobalTableRequest(
            global_table_id='gt-123',
            global_table_name='test-table',
            placements=[
                Placement('cn-shanghai', 'inst-sh', True),
            ],
        )
        proto = self.encoder.encode_request('BindGlobalTable', request)
        self.assertEqual(proto.globalTableId, 'gt-123')
        self.assertEqual(proto.globalTableName, 'test-table')
        self.assertEqual(len(proto.placements), 1)
        self.assertTrue(proto.placements[0].writable)

    def test_encode_unbind_global_table(self):
        request = UnbindGlobalTableRequest(
            global_table_id='gt-123',
            global_table_name='test-table',
            removals=[
                Removal('cn-shanghai', 'inst-sh'),
            ],
        )
        proto = self.encoder.encode_request('UnbindGlobalTable', request)
        self.assertEqual(proto.globalTableId, 'gt-123')
        self.assertEqual(proto.globalTableName, 'test-table')
        self.assertEqual(len(proto.removals), 1)
        self.assertEqual(proto.removals[0].regionId, 'cn-shanghai')

    def test_encode_describe_global_table(self):
        request = DescribeGlobalTableRequest(
            global_table_name='test-table',
            global_table_id='gt-123',
            phy_table=PhyTable('cn-hangzhou', 'inst', 'tbl'),
            return_rpo=True,
        )
        proto = self.encoder.encode_request('DescribeGlobalTable', request)
        self.assertEqual(proto.globalTableName, 'test-table')
        self.assertEqual(proto.globalTableId, 'gt-123')
        self.assertTrue(proto.returnRpo)
        self.assertEqual(proto.phyTable.regionId, 'cn-hangzhou')

    def test_encode_describe_global_table_minimal(self):
        request = DescribeGlobalTableRequest(global_table_name='test-table')
        proto = self.encoder.encode_request('DescribeGlobalTable', request)
        self.assertEqual(proto.globalTableName, 'test-table')

    def test_encode_update_global_table(self):
        request = UpdateGlobalTableRequest(
            global_table_id='gt-123',
            global_table_name='test-table',
            phy_table=UpdatePhyTable(
                region_id='cn-shanghai',
                instance_name='inst-sh',
                table_name='test-table',
                writable=False,
                primary_eligible=True,
            ),
        )
        proto = self.encoder.encode_request('UpdateGlobalTable', request)
        self.assertEqual(proto.globalTableId, 'gt-123')
        self.assertEqual(proto.globalTableName, 'test-table')
        self.assertFalse(proto.phyTable.writable)
        self.assertTrue(proto.phyTable.primaryEligible)

    def test_encode_update_global_table_empty_id(self):
        request = UpdateGlobalTableRequest(
            global_table_id='',
            global_table_name='test-table',
            phy_table=UpdatePhyTable('cn-shanghai', 'inst', 'tbl', writable=True),
        )
        with self.assertRaises(OTSClientError):
            self.encoder.encode_request('UpdateGlobalTable', request)

    def test_encode_update_global_table_no_update_item(self):
        request = UpdateGlobalTableRequest(
            global_table_id='gt-123',
            global_table_name='test-table',
            phy_table=UpdatePhyTable('cn-shanghai', 'inst', 'tbl'),
        )
        with self.assertRaises(OTSClientError):
            self.encoder.encode_request('UpdateGlobalTable', request)


class GlobalTableDecoderTest(unittest.TestCase):
    """Unit tests for Global Table decoder."""

    def setUp(self):
        from tablestore.decoder import OTSProtoBufferDecoder
        self.decoder = OTSProtoBufferDecoder('utf8')

    def test_decode_create_global_table(self):
        import tablestore.protobuf.global_table_pb2 as gt_pb2
        proto = gt_pb2.CreateGlobalTableResponse()
        proto.globalTableId = 'gt-abc-123'
        proto.status = gt_pb2.G_ACTIVE
        body = proto.SerializeToString()

        response, _ = self.decoder.decode_response('CreateGlobalTable', body, 'req-001')
        self.assertEqual(response.global_table_id, 'gt-abc-123')
        self.assertEqual(response.request_id, 'req-001')

    def test_decode_bind_global_table(self):
        import tablestore.protobuf.global_table_pb2 as gt_pb2
        proto = gt_pb2.BindGlobalTableResponse()
        proto.globalTableId = 'gt-abc-123'
        proto.status = gt_pb2.G_ACTIVE
        body = proto.SerializeToString()

        response, _ = self.decoder.decode_response('BindGlobalTable', body, 'req-002')
        self.assertEqual(response.request_id, 'req-002')

    def test_decode_unbind_global_table(self):
        import tablestore.protobuf.global_table_pb2 as gt_pb2
        proto = gt_pb2.UnbindGlobalTableResponse()
        proto.globalTableId = 'gt-abc-123'
        proto.status = gt_pb2.G_ACTIVE
        body = proto.SerializeToString()

        response, _ = self.decoder.decode_response('UnbindGlobalTable', body, 'req-003')
        self.assertEqual(response.request_id, 'req-003')

    def test_decode_describe_global_table(self):
        import tablestore.protobuf.global_table_pb2 as gt_pb2
        proto = gt_pb2.DescribeGlobalTableResponse()
        proto.globalTableId = 'gt-abc-123'
        proto.status = gt_pb2.G_ACTIVE
        proto.serveMode = gt_pb2.PRIMARY_SECONDARY

        phy = proto.phyTables.add()
        phy.regionId = 'cn-hangzhou'
        phy.instanceName = 'inst-hz'
        phy.tableName = 'test-table'
        phy.writable = True
        phy.status = gt_pb2.PHY_ACTIVE
        phy.stage = gt_pb2.SYNC_INCR
        phy.role = 'primary'
        phy.statusTimestamp = 1234567890
        phy.tableId = 'tid-001'

        phy2 = proto.phyTables.add()
        phy2.regionId = 'cn-shanghai'
        phy2.instanceName = 'inst-sh'
        phy2.tableName = 'test-table'
        phy2.writable = False
        phy2.status = gt_pb2.PHY_SYNCDATA
        phy2.stage = gt_pb2.SYNC_FULL
        phy2.role = 'secondary'

        body = proto.SerializeToString()

        response, _ = self.decoder.decode_response('DescribeGlobalTable', body, 'req-004')
        self.assertEqual(response.global_table_id, 'gt-abc-123')
        self.assertEqual(response.status, GlobalTableStatus.ACTIVE)
        self.assertEqual(response.serve_mode, ServeMode.PRIMARY_SECONDARY)
        self.assertEqual(len(response.phy_tables), 2)

        table1 = response.phy_tables[0]
        self.assertEqual(table1.region_id, 'cn-hangzhou')
        self.assertEqual(table1.instance_name, 'inst-hz')
        self.assertEqual(table1.table_name, 'test-table')
        self.assertTrue(table1.writable)
        self.assertEqual(table1.status, PhyTableStatus.ACTIVE)
        self.assertEqual(table1.stage, PhyTableSyncStage.INCR)
        self.assertEqual(table1.role, 'primary')
        self.assertEqual(table1.status_timestamp, 1234567890)
        self.assertEqual(table1.table_id, 'tid-001')

        table2 = response.phy_tables[1]
        self.assertEqual(table2.region_id, 'cn-shanghai')
        self.assertEqual(table2.status, PhyTableStatus.SYNCDATA)
        self.assertEqual(table2.stage, PhyTableSyncStage.FULL)
        self.assertEqual(table2.role, 'secondary')

    def test_decode_describe_global_table_with_optional_fields(self):
        import tablestore.protobuf.global_table_pb2 as gt_pb2
        proto = gt_pb2.DescribeGlobalTableResponse()
        proto.globalTableId = 'gt-xyz'
        proto.status = gt_pb2.G_INIT

        phy = proto.phyTables.add()
        phy.regionId = 'cn-hangzhou'
        phy.instanceName = 'inst'
        phy.tableName = 'tbl'
        phy.writable = False
        phy.isFailed = True
        phy.message = 'sync error'
        phy.rpoNanos = 999999

        body = proto.SerializeToString()

        response, _ = self.decoder.decode_response('DescribeGlobalTable', body, 'req-005')
        self.assertEqual(response.status, GlobalTableStatus.INIT)
        self.assertIsNone(response.serve_mode)

        table = response.phy_tables[0]
        self.assertTrue(table.is_failed)
        self.assertEqual(table.message, 'sync error')
        self.assertEqual(table.rpo_nanos, 999999)

    def test_decode_update_global_table(self):
        import tablestore.protobuf.global_table_pb2 as gt_pb2
        proto = gt_pb2.UpdateGlobalTableResponse()
        body = proto.SerializeToString()

        response, _ = self.decoder.decode_response('UpdateGlobalTable', body, 'req-006')
        self.assertEqual(response.request_id, 'req-006')


if __name__ == '__main__':
    unittest.main()
