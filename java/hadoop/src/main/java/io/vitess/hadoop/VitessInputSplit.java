/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.hadoop;

import com.google.common.io.BaseEncoding;

import io.vitess.proto.Vtgate.SplitQueryResponse;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
