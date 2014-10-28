package com.youtube.vitess.vtgate.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.gson.Gson;
import com.youtube.vitess.vtgate.Query;

/**
 * {@link VitessInputSplit} has a Query corresponding to a set of rows from a Vitess table input
 * source. Locations point to the same VtGate hosts used to fetch the splits. Length information is
 * only approximate.
 *
 */
public class VitessInputSplit extends InputSplit implements Writable {
  private String[] locations;
  private Query query;
  private long length;
  private final Gson gson = new Gson();

  public VitessInputSplit(Query query, long length) {
    this.query = query;
    this.length = length;
  }

  public VitessInputSplit() {

  }

  public Query getQuery() {
    return query;
  }

  public void setLocations(String[] locations) {
    this.locations = locations;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return locations;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    query = gson.fromJson(input.readUTF(), Query.class);
    length = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(gson.toJson(query));
    output.writeLong(length);
  }
}
