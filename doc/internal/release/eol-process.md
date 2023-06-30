# End-of-Life Process

The lifespan of a major version is one year long, after that time, the version has reached its end-of-life.
To properly deprecate a major of Vitess follow the following steps:

- **Update the website documentation**
  > - In the ['Releases' documentation](https://vitess.io/docs/releases/), the EOL version must be moved under the ['Archived Releases' section](https://vitess.io/docs/releases/#archived-releases).
  > - The sidebar of the website must be changed. We need to remove the EOL version from it. To do so, we move the version folder onto the `archive` folder.
- **Delete the `Backport To: ...` label**
  > - Delete the corresponding label for the EOL version, we do not want to motivate anymore backport to the EOL release branch.
- **Make proper announcement on Slack**
  > - Notify the community of this deprecation. 