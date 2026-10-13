# Java Packages

We publish binary packages for our [JDBC driver and Java client on Maven Central](https://central.sonatype.com/namespace/io.vitess).

To do so, we use the [Central Portal](https://central.sonatype.com/).
The `central-publishing-maven-plugin`, configured in the `release` profile of `java/pom.xml`, uploads ("deploys") the packages there as a single bundle.
After the Central Portal validates the bundle, it automatically publishes ("releases") the packages to Maven Central.
Publishing usually takes several minutes, but the packages may take longer to show up in search results.

## Access to the Central Portal

[Create a Central Portal account.](https://central.sonatype.org/register/central-portal/)
Then you must be given access to our `io.vitess` namespace.
The Central Portal does not yet let namespace members invite other users, so ask [Central Support](https://central.sonatype.org/faq/get-support/) to add you.

## One-time setup

### Set up GPG

Follow [Sonatype's GPG instructions](https://central.sonatype.org/publish/requirements/gpg/).

Install `gpg-agent` (needed below) e.g. on Ubuntu via: `sudo apt-get install gnupg-agent`.
for Mac you need to install 'gnupg' via 'brew install gnupg'

### Login configuration

[Generate a user token](https://central.sonatype.org/publish/generate-portal-token/) in the Central Portal.
Then add it to the `settings.xml` in the `$HOME/.m2/` directory, using the server ID `central`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username><!-- token username --></username>
      <password><!-- token password --></password>
    </server>
  </servers>
</settings>
```

## Maven 3.10 and `java/.mvn/maven.config`

`java/.mvn/maven.config` sets two Maven Resolver options for every Maven build under `java/`.
Without them, Maven 3.10 adds `_remote.repositories` and `maven-metadata-local.xml` files to the bundle, and the Central Portal rejects it with `Bundle has content that does NOT have a .pom file`.
See [#21566](https://github.com/vitessio/vitess/issues/21566) and [apache/maven#13388](https://github.com/apache/maven/issues/13388).
Remove the file once `central-publishing-maven-plugin` no longer needs it.
