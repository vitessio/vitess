# Java Vtocc JDBC Driver

Java JDBC driver that is intended to be as a drop-in replacement
for MySQL JDBC driver to connect directly to Vtocc server.

Intended to replace configuration, monitoring and connection pooling
for MySQL connection on the client side.

## How to Use

* Install dependencies by running `../bootstrap.sh`
* Try it out with `./vtocc-client`
  * Run from command line `java -jar ./vtocc-client/target/vtocc-client-*-jar-with-dependencies.jar`
  * Check source code at `./vtocc-client/src/main/java/`
* Add dependency to your project.
  * If you're using Maven: dependency `com.github.youtube.vitess.jdbcdriver`,
    notice that it's not propagated to maven central yet
  * Not using maven? Use `./vtocc-jdbc-driver/target/vtocc-jdbc-driver-*-jar-with-dependencies.jar`
* Configure to use this driver instead of a MySQL one.
  * If you define driver class like`com.mysql.jdbc.Driver` in your configuration,
    replace it with `com.github.youtube.vitess.Driver`
  * If you're using driver manager-based configuration:
    * Register driver: `new com.github.youtube.vitess.Driver()`
    * Use urls like this one to create connections: `jdbc:vtocc://localhost:666/keyspace`
  * If you're creating your connections manually
    use `new com.github.youtube.vitess.Driver().connect(url)`

## Caveats

* Be aware that MySQL database connection configuration would be shifted to
  Vtocc startup parameters from your application.
* Connection between your application and Vtocc is inherently insecure
  as it does not require user or password.
