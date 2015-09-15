# Testing On A Ramdisk

The `integration_test` testsuite contains tests that may time-out if run against a slow disk. If your workspace lives on hard disk (as opposed to [SSD](http://en.wikipedia.org/wiki/Solid-state_drive)), it is recommended that you run tests using a [ramdisk](http://en.wikipedia.org/wiki/RAM_drive).

# Setup

First, set up a normal vitess development environment by running `bootstrap.sh` and sourcing `dev.env` (see [GettingStarted](GettingStarted.md)). Then overwrite the testing temporary directories and make a 4GiB (smaller sizes may work, if you're constrained on RAM) ramdisk at the location of your choice (this example uses `/tmp/vt`):

```sh
export VT_TEST_TMPDIR=/tmp/vt

mkdir ${VT_TEST_TMPDIR}
sudo mount -t tmpfs -o size=4g tmpfs ${VT_TEST_TMPDIR}

export VTDATAROOT=${VT_TEST_TMPDIR}
export TEST_UNDECLARED_OUTPUTS_DIR=${VT_TEST_TMPDIR}
```

You can now run tests (either individually or as part of `make test`) normally.

# Teardown

When you are done testing, you can remove the ramdisk by unmounting it and then removing the directory:

```sh
sudo umount ${VT_TEST_TMPDIR}
rmdir ${VT_TEST_TMPDIR}
```
