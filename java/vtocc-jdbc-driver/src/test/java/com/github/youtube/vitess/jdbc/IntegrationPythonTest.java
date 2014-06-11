package com.github.youtube.vitess.jdbc;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

import java.io.InputStream;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Runs Vtocc python tests from {@code test/queryservice_test.py} against our driver.
 *
 * We use Jython to run Python inside JVM and call to our driver. Jython lacks MySQLdb support so we
 * load zxJDBC to call to our driver instead.
 */
@RunWith(JUnit4.class)
public class IntegrationPythonTest {

  private String targetTestsOutputDirectory = this.getClass().getResource("/").getPath();

  /**
   * Make sure that we understand {@code mysql:} prefix used in python tests.
   */
  @BeforeClass
  public static void setupJdbcDriver() throws Exception {
    DriverManager.registerDriver(new com.github.youtube.vitess.jdbc.Driver());
  }

  private PythonInterpreter pythonInterpreter;

  /**
   * Setting environment variables and path using {@code dev.env}.
   */
  @Before
  public void setupPythonInterpreter() throws Exception {
    PySystemState pySystemState = new PySystemState();
    pySystemState.path.insert(0, new PyString(targetTestsOutputDirectory));
    pySystemState.setCurrentWorkingDir(targetTestsOutputDirectory);
    pySystemState.argv.append(new PyString("--verbose"));
    pySystemState.argv.append(new PyString("-e"));
    pySystemState.argv.append(new PyString("vtocc"));
    pythonInterpreter = new PythonInterpreter(null, pySystemState);
    pythonInterpreter.setOut(System.out);
    pythonInterpreter.setErr(System.err);
    // Jython loads environment from immutable System.getenv(), so we need to override it
    try (InputStream devEnvPropertiesStream =
        this.getClass().getResourceAsStream("/dev.env.properties")) {
      Properties devEnvProperties = new Properties();
      devEnvProperties.load(devEnvPropertiesStream);
      for (Object key : devEnvProperties.keySet()) {
        pythonInterpreter.exec("import os \n"
            + "os.environ['"+ key +"'] = '" + devEnvProperties.get(key) + "'");
      }
    }
  }

  @Test
  public void testQueryServiceTest() throws Exception {
    pythonInterpreter.execfile("queryservice_test.py");
  }
}
