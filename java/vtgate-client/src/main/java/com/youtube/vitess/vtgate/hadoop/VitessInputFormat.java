package com.youtube.vitess.vtgate.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.hadoop.writables.KeyspaceIdWritable;
import com.youtube.vitess.vtgate.hadoop.writables.RowWritable;

/**
 * {@link VitessInputFormat} is the {@link InputFormat} for tables in Vitess.
 * Input splits ({@link VitessInputSplit}) are fetched from VtGate via an RPC.
 * map() calls are supplied with a {@link KeyspaceIdWritable},
 * {@link RowWritable} pair.
 */
public class VitessInputFormat extends
		InputFormat<KeyspaceIdWritable, RowWritable> {

	@Override
	public List<InputSplit> getSplits(JobContext context) {
		try {
			VitessConf conf = new VitessConf(context.getConfiguration());
			VtGate vtgate = VtGate
					.connect(conf.getHosts(), conf.getTimeoutMs());
			List<InputSplit> splits = vtgate.getMRSplits(conf.getKeyspace(),
					conf.getInputTable(),
					conf.getInputColumns());
			for (InputSplit split : splits) {
				((VitessInputSplit) split).setLocations(conf.getHosts().split(
						VitessConf.HOSTS_DELIM));
			}
			return splits;
		} catch (ConnectionException | DatabaseException e) {
			throw new RuntimeException(e);
		}
	}

	public RecordReader<KeyspaceIdWritable, RowWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new VitessRecordReader();
	}

	/**
	 * Sets the necessary configurations for Vitess table input source
	 */
	public static void setInput(Job job, String hosts, String keyspace,
			String table, List<String> columns) {
		job.setInputFormatClass(VitessInputFormat.class);
		VitessConf vtConf = new VitessConf(job.getConfiguration());
		vtConf.setHosts(hosts);
		vtConf.setKeyspace(keyspace);
		vtConf.setInputTable(table);
		vtConf.setInputColumns(columns);
	}
}
