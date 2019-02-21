# Overview

This subdirectory contains all Vitess Java code.

It is split in the following subdirectories (Maven modules):

* **client:** Our Java client library.
  * See [VTGateConn.java](https://github.com/vitessio/vitess/blob/master/java/client/src/main/java/io/vitess/client/VTGateConn.java) and [VTGateBlockingConn.java](https://github.com/vitessio/vitess/blob/master/java/client/src/main/java/io/vitess/client/VTGateBlockingConn.java) for the API.
  * Note: The library is agnostic of the underlying RPC system and only defines an interface for that.
  * In open-source, the library must always be used together with the code in `grpc-client`.
* **grpc-client:** Implements the client's RPC interface for gRPC.
* **jdbc:** JDBC driver implementation for Vitess.
* **example:** Examples for using the `client` or the `jdbc` module.
* **hadoop:** Vitess support for Hadoop. See [documentation for details](hadoop/src/main/java/io/vitess/hadoop/README.md).

**Note:** The `artifactId` for each module listed above has the prefix `vitess-` i.e. you will have to look for `vitess-jdbc` and not `jdbc`.

TODO(mberlin): Mention Maven Central once we started publishing artifacts there.

# Adding new Dependencies

When submitting contributions which require new dependencies, please follow these guidelines:

* Put every directly used dependency into the module's `dependencies` section (e.g. in `jdbc/pom.xml` for changes to the JDBC code).
  * `make java_test` (which calls `mvn verify` in the `/java` directory) will run `mvn dependency:analyze` and fail if you got this wrong.
* Limit the scope of test dependencies to `<scope>test</scope>`.
* Do not include the version number in the module's pom.xml. Instead, add the dependency to the `dependencyManagement` section in `/java/pom.xml` and include the version number there.
* Sort dependencies in alphabetic order. Modules only: Put all dependencies with limited scope (e.g. `test`) in a separate `block` and sort it alphabetically as well (see `/java/client/pom.xml` for an example).
* Feel free to separate groups of dependencies by newlines (e.g. all io.vitess.* dependencies are a group).

