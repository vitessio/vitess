package com.youtube.vitess.vtgate.integration.util;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class SetupCommand {
  public static List<String> get(TestEnv testEnv, boolean isSetUp) {
    String vtTop = System.getenv("VTTOP");
    if (vtTop == null) {
      Assert.fail("VTTOP is not set");
    }
    List<String> command = new ArrayList<String>();
    command.add("python");
    command.add(vtTop + "/test/java_vtgate_test_helper.py");
    command.add("--shards");
    command.add(testEnv.getShardNames());
    command.add("--tablet-config");
    command.add(testEnv.getTabletConfig());
    command.add("--keyspace");
    command.add(testEnv.keyspace);
    if (isSetUp) {
      command.add("setup");
    } else {
      command.add("teardown");
    }
    return command;
  }
}
