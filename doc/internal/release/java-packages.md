# Java Packages

We publish binary packages for our [JDBC driver and Java client on Maven Central](https://central.sonatype.com/namespace/io.vitess).

To do so, we use the [Central Portal](https://central.sonatype.com/).
The `central-publishing-maven-plugin`, configured in the `release` profile of `java/pom.xml`, uploads ("deploys") the packages there as a single bundle.
It must be version 0.12.0 or later to publish with Maven 3.10 ([#21566](https://github.com/vitessio/vitess/issues/21566)).
After the Central Portal validates the bundle, it automatically publishes ("releases") the packages to Maven Central.
Publishing usually takes several minutes, but the packages may take longer to show up in search results.

## Access to the Central Portal

[Create a Central Portal account.](https://central.sonatype.org/register/central-portal/)
Then you must be given access to our `io.vitess` namespace.
Ask an administrator of the organization that owns `io.vitess` to [invite you and grant you access to the namespace](https://central.sonatype.org/publish/publish-portal-organizations/).
If they don't see the Organizations tab, they can ask [Central Support](https://central.sonatype.org/faq/get-support/) to enable organization management.

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
    export MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED"
    mvn clean deploy -P release
    ```
