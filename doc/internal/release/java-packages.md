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

## Publishing a release

> **Warning:** Once the packages are published, you cannot delete them. The only option is to publish a newer version (e.g. increment the patch level).

1. Check out the release tag, e.g. `git checkout v24.0.5`.
2. Start `gpg-agent`, so that Maven doesn't keep prompting for your private key's passphrase (see [How to Release](how-to-release.md#java-packages-deploy--release)).
3. Go into the `java` directory, then build and publish the packages:

    ```bash
    cd java
    export MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED -Daether.priority.EnhancedLocalRepositoryManagerFactory=NaN -Daether.priority.VersionsMetadataGeneratorFactory=NaN -Daether.syncContext.named.factory=rwlock-local"
    mvn clean deploy -P release
    ```

## Maven 3.10

The three `-Daether` options in `MAVEN_OPTS` are needed with Maven 3.10.
Without them, Maven 3.10 adds `_remote.repositories` and `maven-metadata-local.xml` files to the bundle, and the Central Portal rejects it with `Bundle has content that does NOT have a .pom file`.
They are the release-only workaround from [apache/maven#13388](https://github.com/apache/maven/issues/13388); see also [#21566](https://github.com/vitessio/vitess/issues/21566).
They don't change the bundle that Maven 3.9 builds, so the same command works with either version.
Other Maven builds don't need them.
