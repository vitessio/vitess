# Contributing to Vitess
> We expect you to know how to use Git and GitHub at least on the basic level. Check [GitHub tuturial](https://help.github.com/articles/set-up-git) for more information.

Fork our repo, make your change, request for a pull, and you've got your approval in no time. That's easy!

Unless your change is large or complicated, then you'll need to follow same practicies that
vitess team does. Your code would be reviewed on http://codereview.appspot.com

Your contribution is welcome, and we highly appreciate your efforts on making vitess better.

## Instructions
### For Small Fixes
- Subscribe to https://groups.google.com/forum/#!forum/vitess-issues.
- [Install vitess](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown)
- Make a pull request with your change in a normal github fashion, [fork our repo](https://help.github.com/articles/fork-a-repo).
- Usually we respond in one business day or less, project is very active

### CodeReview for Bigger Contributions
In case your change is big and non-trivial, we'll need to do a proper code review.
(We try hard to have a nice and pretty code base)
We use a separate tool for that, which is not related to github and only uploads you local
git changes for review.

- Be sure to follow steps for small fixes first.
- Create an account at http://codereview.appspot.com.
- Download [upload.py](https://code.google.com/p/rietveld/wiki/UploadPyUsage) and put it in your path.
- Use `$VTTOP/misc/git/createcl` script to submit local changes for review (it's convenient to put it on your `$PATH`)
  - When running for the first time your Google account's *application-specific* password would be required. Link to the page where you can create new one is provided.
  - It's a small script diffs beween current branch and `master` one, uploads it onto codereview server and asks for a code review. Vitess team is notified, so we'll be able to start working on it promptly.
