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

package io.vitess.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import vttest.Vttest.VTTestTopology;

/**
 * Helper class to hold the configurations for VtGate setup used in integration tests
 */
public class TestEnv {

  private VTTestTopology topology;
  private String keyspace;
  private String outputPath;
  private Process pythonScriptProcess;
  private int port;

  public void setTopology(VTTestTopology topology) {
    this.topology = topology;
  }

  public VTTestTopology getTopology() {
    return this.topology;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getKeyspace() {
    return this.keyspace;
  }

  public int getPort() {
    return this.port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public Process getPythonScriptProcess() {
    return this.pythonScriptProcess;
  }

  public void setPythonScriptProcess(Process process) {
    this.pythonScriptProcess = process;
  }

  /**
   * Get setup command to launch a cluster.
   */
  public List<String> getSetupCommand(int port) {
    String vtTop = System.getenv("VTTOP");
    if (vtTop == null) {
      throw new RuntimeException("cannot find env variable: VTTOP");
    }
    String schemaDir = getTestDataPath() + "/schema";
    List<String> command = new ArrayList<String>();
    command.add(vtTop + "/py/vttest/run_local_database.py");
    command.add("--port");
    command.add(Integer.toString(port));
    command.add("--proto_topo");
    command.add(getTopology().toString());
    command.add("--schema_dir");
    command.add(schemaDir);
    return command;
  }

  public String getTestDataPath() {
    String vtTop = System.getenv("VTTOP");
    if (vtTop == null) {
      throw new RuntimeException("cannot find env variable: VTTOP");
    }
    return vtTop + "/data/test";
  }

  public String getTestOutputPath() {
    if (outputPath == null) {
      try {
        outputPath = Files.createTempDirectory("vttest").toString();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return outputPath;
  }

  public void clearTestOutput() throws IOException {
    if (outputPath != null) {
      FileUtils.deleteDirectory(new File(outputPath));
    }
  }
}
