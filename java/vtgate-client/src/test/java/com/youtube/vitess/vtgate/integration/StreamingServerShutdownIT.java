package com.youtube.vitess.vtgate.integration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;

public class StreamingServerShutdownIT {

	static VtGateParams params;

	@Before
	public void setUpVtGate() throws Exception {
		params = Util.runVtGate(true);
		Util.truncateTable(params);
	}

	@After
	public void tearDownVtGate() throws Exception {
		Util.runVtGate(false);
	}

	@Test
	public void testShutdownServerWhileStreaming() throws Exception {
		Util.insertRows(params, 1, 2000);
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select A.* from vtgate_test A join vtgate_test B";
		Query joinQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).withStream(true).build();
		Cursor cursor = vtgate.execute(joinQuery);

		int count = 0;
		try {
			while (cursor.hasNext()) {
				count++;
				if (count == 1) {
					Util.runVtGate(false);
				}
				cursor.next();
			}
			vtgate.close();
			Assert.fail("failed to raise exception");
		} catch (RuntimeException e) {
			Assert.assertTrue(e.getMessage().contains(
					"vtgate exception: connection exception"));
		}
	}
}
