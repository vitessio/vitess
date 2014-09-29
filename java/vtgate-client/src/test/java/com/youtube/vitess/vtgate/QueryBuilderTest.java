package com.youtube.vitess.vtgate;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.youtube.vitess.vtgate.Query.QueryBuilder;

public class QueryBuilderTest {
	@Test
	public void testValidQueryWithKeyspaceIds() {
		String sql = "select 1 from dual";
		KeyspaceId kid = KeyspaceId.valueOf("80");
		QueryBuilder builder = new QueryBuilder("select 1 from dual",
				"test_keyspace", "master").withAddedKeyspaceId(kid);
		Query query = builder.build();
		Assert.assertEquals(sql, query.getSql());
		Assert.assertEquals("test_keyspace", query.getKeyspace());
		Assert.assertEquals("master", query.getTabletType());
		Assert.assertEquals(null, query.getBindVars());
		Assert.assertEquals(1, query.getKeyspaceIds().size());
		Assert.assertTrue(Arrays.equals(kid.getBytes(), query.getKeyspaceIds()
				.get(0)));
		Assert.assertEquals(null, query.getKeyRanges());
	}

	@Test
	public void testValidQueryWithKeyRanges() {
		String sql = "select 1 from dual";
		QueryBuilder builder = new QueryBuilder("select 1 from dual",
				"test_keyspace", "master").withAddedKeyRange(KeyRange.ALL);
		Query query = builder.build();
		Assert.assertEquals(sql, query.getSql());
		Assert.assertEquals("test_keyspace", query.getKeyspace());
		Assert.assertEquals("master", query.getTabletType());
		Assert.assertEquals(null, query.getBindVars());
		Assert.assertEquals(1, query.getKeyRanges().size());
		Assert.assertEquals(KeyRange.ALL.toMap(), query.getKeyRanges().get(0));
		Assert.assertEquals(null, query.getKeyspaceIds());
	}

	@Test
	public void testNoKeyspaceIdOrKeyrange() {
		QueryBuilder builder = new QueryBuilder("select 1 from dual",
				"test_keyspace", "master");
		try {
			builder.build();
			Assert.fail("did not raise IllegalStateException");
		} catch (IllegalStateException e) {
			Assert.assertEquals(
					"query must have either keyspaceIds or keyRanges",
					e.getMessage());
		}
	}

	@Test
	public void testBothKeyspaceIdAndKeyrange() {
		QueryBuilder builder = new QueryBuilder("select 1 from dual",
				"test_keyspace", "master").withAddedKeyRange(KeyRange.ALL)
				.withAddedKeyspaceId(KeyspaceId.valueOf("80"));
		try {
			builder.build();
			Assert.fail("did not raise IllegalStateException");
		} catch (IllegalStateException e) {
			Assert.assertEquals(
					"query cannot have both keyspaceIds and keyRanges",
					e.getMessage());
		}
	}
}
