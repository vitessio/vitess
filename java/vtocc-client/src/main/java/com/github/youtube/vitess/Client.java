package com.github.youtube.vitess;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

import com.github.youtube.vitess.jdbc.bson.Driver;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Reimplementation of {@code vtclient2} from vitess distribution.
 *
 * Supports only a subset of options.
 */
public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  @SuppressWarnings("AccessStaticViaInstance")
  public static void main(String[] args) {
    CommandLineOptions options = null;
    try {
      options = CommandLineOptions.parseCommandLine(args);
      if (options.count != 1) {
        throw new IllegalArgumentException("--count is not supported");
      }
      if (!options.bindvars.isEmpty()) {
        throw new IllegalArgumentException("--bindvars are not supported");
      }
    } catch (ParseException e) {
      logger.error("Can not parse command line string", e);
      CommandLineOptions.printHelp();
      System.exit(1);
    }

    Connection connection = null;
    try {
      // Loading and registering Driver in DriverManager
      new Driver();
      connection = DriverManager.getConnection("jdbc:vtocc://" + options.server);
    } catch (SQLException | RuntimeException e) {
      logger.error("client error: ", e);
      System.exit(1);
    }
    try {
      executeQuery(connection, options.sql, options.verbose);
    } catch (SQLException | RuntimeException e) {
      logger.error("client error: ", e);
      System.exit(1);
    }
  }

  private static void executeQuery(Connection connection, String sql, boolean verbose)
      throws SQLException {
    logger.info("Sending the query...");
    Stopwatch queryStopwatch = Stopwatch.createStarted();

    ResultSet resultSet = connection.prepareStatement(sql).executeQuery();

    if (verbose) {
      StringBuilder line = new StringBuilder("Index");
      for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
        line.append('\t').append(resultSet.getMetaData().getCatalogName(i));
      }
      logger.info(line.toString());
    }

    int rowIndex = 0;
    while (resultSet.next()) {
      if (verbose) {
        StringBuilder line = new StringBuilder(Integer.toString(rowIndex));
        for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
          Object value = resultSet.getObject(i);
          if (value == null) {
            line.append('\t');
          } else {
            line.append('\t').append(value.toString());
          }
        }
        logger.info(line.toString());
      }
      rowIndex++;
    }
    logger.info("Total time: {}ms / Row count: {}",
        (float) queryStopwatch.elapsed(TimeUnit.NANOSECONDS) / TimeUnit.MILLISECONDS.toNanos(1),
        rowIndex);
  }

  /**
   * Parses command line parameters into a convenient class.
   */
  public static final class CommandLineOptions {

    @SuppressWarnings("AccessStaticViaInstance")
    private static final Options options = new Options()
        .addOption(OptionBuilder.withLongOpt("count").hasArg()
            .withDescription("how many times to run the query").create())
        .addOption(OptionBuilder.withLongOpt("bindvars").withValueSeparator().hasArg()
            .withDescription("bind vars as a json dictionary").create())
        .addOption(OptionBuilder.withLongOpt("server").hasArg()
            .withDescription(
                "vtocc server as hostname:port/keyspace")
            .create())
        .addOption(OptionBuilder.withLongOpt("driver").hasArg()
            .withDescription(
                "which driver to use (one of vttable, vttablet-streaming, vtdb, vtdb-streaming)")
            .create())
        .addOption(
            OptionBuilder.withLongOpt("verbose").hasArg().withDescription("show results").create());
    public final int count;
    public final Map<String, String> bindvars;
    public final String server;
    public final boolean verbose;
    public final String sql;

    public CommandLineOptions(int count, Map<String, String> bindvars, String server,
        boolean verbose, String sql) {
      this.count = count;
      this.bindvars = bindvars;
      this.server = server;
      this.verbose = verbose;
      this.sql = sql;
    }

    public static CommandLineOptions parseCommandLine(String[] args) throws ParseException {
      CommandLine line = new BasicParser().parse(options, args);
      int count = Integer.parseInt(line.getOptionValue("count", "1"));
      Map<String, String> bindvars = Maps.fromProperties(line.getOptionProperties("bindvars"));
      String server = line.getOptionValue("server", "localhost:6603/test_keyspace");
      boolean verbose = Boolean.parseBoolean(line.getOptionValue("verbose", "false"));
      String sql = Joiner.on(' ').join(line.getArgList());
      return new CommandLineOptions(count, bindvars, server, verbose, sql);
    }

    public static void printHelp() {
      new HelpFormatter().printHelp("java -jar vtocc-client.jar", options);
    }
  }
}
