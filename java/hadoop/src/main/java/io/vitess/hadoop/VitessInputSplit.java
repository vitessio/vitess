package io.vitess.hadoop;

import com.google.common.io.BaseEncoding;
import io.vitess.proto.Vtgate.SplitQueryResponse;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class VitessInputSplit extends InputSplit implements Writable {
  private String[] locations;
  private SplitQueryResponse.Part split;

  public VitessInputSplit(SplitQueryResponse.Part split) {
    this.split = split;
  }

  public VitessInputSplit() {
  }

  public SplitQueryResponse.Part getSplit() {
    return split;
  }

  public void setLocations(String[] locations) {
    this.locations = locations;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return split.getSize();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return locations;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    split = SplitQueryResponse.Part.parseFrom(BaseEncoding.base64().decode(input.readUTF()));
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(BaseEncoding.base64().encode(split.toByteArray()));
  }
}
