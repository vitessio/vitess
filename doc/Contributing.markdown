# Contributing to Vitess

If you'd like to make simple contributions to Vitess, we recommend that you fork
the repository and submit pull requests. If you'd like to make larger or ongoing
changes, you'll need to follow a similar set of processes and rules that the
Vitess team follows.

### Prerequisites
  - [Install vitess](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown)
  - The vitess team uses appspot for code reviews. You'll need to create an account at http://codereview.appspot.com.
  - Fork the vitess repository, say https://github.com/myfork/vitess.
  - Download [upload.py](https://code.google.com/p/rietveld/wiki/UploadPyUsage) and put it in your path.



## Small changes

For small, well contained changes, just send us a
[pull request](https://help.github.com/articles/using-pull-requests).

## Bigger changes

If you are planning to make bigger changes or add serious features to Vitess, we
ask you to follow our code review process.

### Recommended Git flow: single contributor

Use your fork as a push-only staging ground for submitting pull requests. The
assumption is that you'll never have to fetch from the fork. If this is the
case, all you have to do is configure your local repository to pull from
youtube, and push to myfork. This can be achieved as follows:

```
~/...vitess> git remote -v
origin  git@github.com:youtube/vitess.git (fetch)
origin  git@github.com:youtube/vitess (push)
~/...vitess> git remote set-url --push origin git@github.com:myfork/vitess
~/...vitess> git remote -v
origin  git@github.com:youtube/vitess.git (fetch)
origin  git@github.com:myfork/vitess (push)
```

The limitation of this configuration is that you can only pull from the youtube
repository. The `git pull` command will `fetch` from youtube/vitess and `merge`
into your master branch.

On the other hand, `git push` will push into your myfork/vitess remote.

The advantage of this workflow is that you don't have to worry about specifying
where you're pulling from or pushing to because the default settings *do the
right thing*.

### Recommended Git flow: multiple contributors

If more than one of you plan on contributing through a single fork, then you'll
need to follow a more elaborate scheme of setting up multiple remotes and
manually managing merges:

```
~/...vitess> git remote -v
origin  git@github.com:youtube/vitess.git (fetch)
origin  git@github.com:youtube/vitess.git (push)
~/...vitess> git remote add myfork git@github.com:myfork/vitess.git
~/...vitess> git remote -v
myfork  git@github.com:myfork/vitess.git (fetch)
myfork  git@github.com:myfork/vitess.git (push)
origin  git@github.com:youtube/vitess.git (fetch)
origin  git@github.com:youtube/vitess.git (push)
```

With this setup, commands like `git pull` and `git push` with default settings
are not recommended. You will be better off using `git fetch` and `git merge`,
which let you micromanage your remote interactions. For example, you'll need to
`git push myfork` to explicitly push your changes to myfork.

### Changes and code reviews
We recommend that you make your changes in a separate branch.
Make sure you're on the master branch when you create it.

```
~/...vitess> git status
# On branch master
# Your branch is up-to-date with 'origin/master'.
#
nothing to commit, working directory clean
~/...vitess> git checkout -b newfeature
Switched to a new branch 'newfeature'
```
Once your changes are ready for review and committed into your branch,
you can run the createcl tool, for example:
```
createcl -r alainjobart
```
This command will automatically run a diff of the current branch `newfeature`
against `master` and create an appspot code review with `alainjobart` as
reviewer. vitess-issues will be cc'd. If necessary, createcl allows you to
specify the exact versions to diff. (but we recommend that you don't use those).

After getting feedback about your code, you can update the code by calling

```
createcl -i 12345
```

with the actual id of your change instead of 12345.

During your feature development, you can fetch and merge new changes from the main youtube repository.
If you choose to do so, make sure you merge the changes to both the `master` and `newfeature` branches.
In the sole contributor case, your commands will look like this:
```
git checkout master
git pull
git checkout newfeature
git merge master
```
Once your change is approved, push and submit it as a pull request:
```
git checkout master
git merge newfeature
git push
```
The above commands will merge `newfeature` into `master` and push the changes to the myfork remote.
You can then go to https://github.com/myfork/vitess to submit the branch as your pull request.
If done correctly, only your changes will show up in the pull request.
github will cancel out changes you merged from youtube master, unless you resolved merge conflicts.

If necessary, you can work on multiple branches at the same time.
When the time comes to submit, you just have to merge the branch onto `master` and push.

## Other Git setups

As you can see above, the only requirement from the Vitess team is that you send
your code reviews through appspot, and then submit the same changes as a pull
request.

Our workflow recommendation is mainly to simplify your life. If you prefer to
use a different workflow, you can choose to do so as long as you can figure out
a way to meet the necessary requirements.
