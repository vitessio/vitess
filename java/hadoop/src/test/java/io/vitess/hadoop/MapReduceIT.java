/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.vitess.client.TestEnv;
import io.vitess.client.TestUtil;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import junit.extensions.TestSetup;
import junit.framework.TestSuite;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import vttest.Vttest.Keyspace;
import vttest.Vttest.Shard;
import vttest.Vttest.VTTestTopology;



/**
 * Integration tests for MapReductions in Vitess. These tests use an in-process Hadoop cluster via
 * {@link HadoopTestCase}. These tests are JUnit3 style because of this dependency. Vitess setup for
 * these tests require at least one rdonly instance per shard.
 *
 */
public class MapReduceIT extends HadoopTestCase {

  private static final int NUM_ROWS = 40;

  public static TestEnv testEnv = getTestEnv();

  public MapReduceIT() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  /**
   * Run a mapper only MR job and verify all the rows in the source table were outputted into HDFS.
   */
  public void testDumpTableToHDFS() throws Exception {
    // Configurations for the job, output from mapper as Text
    Configuration conf = createJobConf();
    Job job = Job.getInstance(conf);
    job.setJobName("table");
    job.setJarByClass(VitessInputFormat.class);
    job.setMapperClass(TableMapper.class);
    VitessInputFormat.setInput(
        job,
        "localhost:" + testEnv.getPort(),
        testEnv.getKeyspace(),
        "select id, name, age from vtgate_test",
        ImmutableList.<String>of(),
        4 /* splitCount */,
        0 /* numRowsPerQueryPart */,
        Algorithm.EQUAL_SPLITS,
        TestUtil.getRpcClientFactory().getClass());
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(RowWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(0);

    Path outDir = new Path(testEnv.getTestOutputPath(), "mrvitess/output");
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());

    String[] outputLines = MapReduceTestUtil.readOutput(outDir, conf).split("\n");
    // there should be one line per row in the source table
    assertEquals(NUM_ROWS, outputLines.length);
    Set<Long> actualAges = new HashSet<>();
    Set<String> actualNames = new HashSet<>();

    // Parse and verify we've gotten all the ages and rows.
    Gson gson = new Gson();
    for (String line : outputLines) {
      String[] parts = line.split("\t");
      actualAges.add(Long.valueOf(parts[0]));

      // Rows are written as JSON since this is TextOutputFormat.
      String rowJson = parts[1];
      Type mapType = new TypeToken<Map<String, String>>() {}.getType();
      @SuppressWarnings("unchecked")
      Map<String, String> map = (Map<String, String>) gson.fromJson(rowJson, mapType);
      actualNames.add(map.get("name"));
    }

    Set<Long> expectedAges = new HashSet<>();
    Set<String> expectedNames = new HashSet<>();
    for (long i = 1; i <= NUM_ROWS; i++) {
      // Generate values that match TestUtil.insertRows().
      expectedAges.add(i % 10);
      expectedNames.add("name_" + i);
    }
    assertEquals(expectedAges.size(), actualAges.size());
    assertTrue(actualAges.containsAll(expectedAges));
    assertEquals(NUM_ROWS, actualNames.size());
    assertTrue(actualNames.containsAll(expectedNames));
  }

  /**
   * Map all rows and aggregate by age at the reducer.
   */
  public void testReducerAggregateRows() throws Exception {
    Configuration conf = createJobConf();

    Job job = Job.getInstance(conf);
    job.setJobName("table");
    job.setJarByClass(VitessInputFormat.class);
    job.setMapperClass(TableMapper.class);
    VitessInputFormat.setInput(
        job,
        "localhost:" + testEnv.getPort(),
        testEnv.getKeyspace(),
        "select id, name, age from vtgate_test",
        ImmutableList.<String>of(),
        1 /* splitCount */,
        0 /* numRowsPerQueryPart */,
        Algorithm.EQUAL_SPLITS,
        TestUtil.getRpcClientFactory().getClass());

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(RowWritable.class);

    job.setReducerClass(CountReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    Path outDir = new Path(testEnv.getTestOutputPath(), "mrvitess/output");
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());

    String[] outputLines = MapReduceTestUtil.readOutput(outDir, conf).split("\n");
    // There should be 10 different ages, because age = i % 10.
    assertEquals(10, outputLines.length);
    // All rows should be accounted for.
    int totalRowsReduced = 0;
    for (String line : outputLines) {
      totalRowsReduced += Integer.parseInt(line);
    }
    assertEquals(NUM_ROWS, totalRowsReduced);
  }

  public static class TableMapper
      extends Mapper<NullWritable, RowWritable, IntWritable, RowWritable> {
    @Override
    public void map(NullWritable key, RowWritable value, Context context)
        throws IOException, InterruptedException {
      // Tag each record with its age.
      try {
        context.write(new IntWritable(value.get().getInt("age")), value);
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  public static class CountReducer
      extends Reducer<IntWritable, RowWritable, NullWritable, IntWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<RowWritable> values, Context context)
        throws IOException, InterruptedException {
      // Count how many records there are for each age.
      int count = 0;
      Iterator<RowWritable> itr = values.iterator();
      while (itr.hasNext()) {
        count++;
        itr.next();
      }
      context.write(NullWritable.get(), new IntWritable(count));
    }
  }

  /**
   * Create env with two shards each having a master, replica, and rdonly.
   */
  static TestEnv getTestEnv() {
    Keyspace keyspace = Keyspace.newBuilder().setName("test_keyspace")
        .addShards(Shard.newBuilder().setName("-80").build())
        .addShards(Shard.newBuilder().setName("80-").build()).build();
    VTTestTopology topology = VTTestTopology.newBuilder().addKeyspaces(keyspace).build();
    TestEnv env = TestUtil.getTestEnv("test_keyspace", topology);
    return env;
  }

  public static TestSetup suite() {
    return new TestSetup(new TestSuite(MapReduceIT.class)) {

      @Override
      protected void setUp() throws Exception {
        TestUtil.setupTestEnv(testEnv);
        // Insert test rows
        TestUtil.insertRows(testEnv, 1, NUM_ROWS);
      }

      @Override
      protected void tearDown() throws Exception {
        TestUtil.teardownTestEnv(testEnv);
      }
    };

  }
}
