# Testing On A Ramdisk

The `integration_test` testsuite contains tests that may time-out if run against a slow disk. If your workspace lives on hard disk (as opposed to [SSD](http://en.wikipedia.org/wiki/Solid-state_drive)), it is recommended that you run tests using a [ramdisk](http://en.wikipedia.org/wiki/RAM_drive).

# Setup

First, set up a normal vitess development environment by running `bootstrap.sh` and sourcing `dev.env` (see [GettingStarted](GettingStarted.markdown)). Then overwrite the testing temporary directories and make a 2GiB ramdisk at the location of your choice (this example uses `/tmp/vt`):

```sh
export TEST_TMPDIR=/tmp/vt

mkdir ${TEST_TMPDIR}
sudo mount -t tmpfs -o size=2g tmpfs ${TEST_TMPDIR}

export VTDATAROOT=${TEST_TMPDIR}
export TEST_UNDECLARED_OUTPUTS_DIR=${TEST_TMPDIR}
```

You can now run tests (either individually or as part of `make test`) normally.

# Teardown

When you are done testing, you can remove the ramdisk by unmounting it and then removing the directory:

```sh
sudo umount ${TEST_TMPDIR}
rmdir ${TEST_TMPDIR}
```
