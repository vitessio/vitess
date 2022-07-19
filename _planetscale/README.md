diff a/_planetscale/README.md b/_planetscale/README.md	(rejected hunks)
@@ -2,11 +2,13 @@
 
 This is PlanetScale's private fork of Vitess with proprietary patches and extensions.
 
-We plan to switch PSDB Cloud and PSDB Operator to run Vitess builds from this fork
-once the continuous builds are configured and tested. This will allow us to deploy
+PSDB Cloud and PSDB Operator run Vitess builds from this fork. This allows us to deploy
 builds with either temporary or permanent changes relative to upstream Vitess,
 such as embargoed security fixes or extra, proprietary features.
 
+Since images built from this fork contain proprietary bits that must be licensed,
+we publish them to an access-controlled registry.
+
 You should _not_ use this fork if you're working on a pull request that will be sent
 to the upstream, open-source [Vitess](https://github.com/vitessio/vitess) repository.
 This fork is only for code that we intend to remain proprietary, either temporarily
@@ -37,14 +39,14 @@ vtpublic        git@github.com:planetscale/vitess.git (push)
 
 ## Fork Guidelines
 
-We intend to rebase upstream Vitess into this fork continuously and automatically.
-To minimize the occurrence of merge conflicts that will need to be fixed manually,
+We intend to rebase this fork on top of upstream Vitess continuously and automatically.
+To minimize the occurrence of conflicts that will need to be fixed manually,
 please follow these guidelines:
 
 1. Whenever possible, avoid modifying files that exist upstream.
 
    These modifications are difficult to track and maintain because they tend to
-   cause merge conflicts and it's hard to tell exactly what we changed relative
+   cause conflicts and it's hard to tell exactly what we changed relative
    to upstream.
 
    In many cases, we should be able to add custom behavior by creating an entirely
@@ -82,15 +84,15 @@ So, this should not grow on us.
 
 The normal developer workflow should not change: create a feature branch
 as usual, push it, create PRs, and merge after review, just as usual.
-The PRs along with the associated branches will essentially by our way
+The PRs along with the associated branches will essentially be our way
 of tracking history.
 
-After the merge:
-* Create a new branch from prior to the commit.
-* Cherry-pick the commit.
-* Squash the cherry-pick with the commit that represents the piece of
-  work it modifies.
+After the PR is merged:
+* Create a new branch from prior to the merge commit.
+* Cherry-pick the commit inside the PR (not the merge commit).
+* Rebase against upstream master and fixup the cherry-pick with the
+  commit that represents the piece of work it modifies.
 * Update the `Annotate` commit, which is a human readable version of
   the current state of the branch.
-* Create a copy the original `main` branch.
-* Replace `main` with the new branch.
+* Create a copy the original `main` branch (git checkout -b copied-branch)
+* Replace `main` with the new branch (git push --force).
