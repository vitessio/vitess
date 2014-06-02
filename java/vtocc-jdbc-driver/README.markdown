# Java Vtocc JDBC Driver

Java JDBC driver that is intended to be as a drop-in replacement
for MySQL JDBC driver to connect directly to Vtocc server.

Intended to replace configuration, monitoring and connection pooling
for MySQL connection on the client side.

## How to Use

* Install dependencies by running `../bootstrap.sh`
* Add dependency to your project
  * If you're using Maven: dependency `com.github.youtube.vitess.jdbcdriver`
  * Not using maven: use `./target/**/*.jar`
* Use this driver instead of a MySQL one
  * **TODO(timofeyb):** actually implement these
  * If you're using driver class like `com.mysql.driver` in your configuration,
    replace with `com.github.youtube.vitess.Driver `
  * If you're creating your connections manually:
    Use `Driver.connection()`
  * If you're using JNDI-based configuration:
    * Register driver in JNDI like this
    * Use JNDI connection string like this:

## Caveats

* Be aware that MySQL database connection configuration would be shifted to
  Vtocc startup parameters from your application
* Connection between your application and Vtocc is inherently insecure
  as it does not require user or password
