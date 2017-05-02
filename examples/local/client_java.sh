#!/bin/bash

# This is a wrapper script that installs and runs the example
# client for the low-level Java interface.

set -e

script_root=`dirname "${BASH_SOURCE}"`

# We have to install the "example" module first because Maven cannot resolve
# them when we run "exec:java". See also: http://stackoverflow.com/questions/11091311/maven-execjava-goal-on-a-multi-module-project
# Install only "example". See also: http://stackoverflow.com/questions/1114026/maven-modules-building-a-single-specific-module
mvn -f $script_root/../../java/pom.xml -pl example -am install -DskipTests
mvn -f $script_root/../../java/example/pom.xml exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="io.vitess.example.VitessClientExample" -Dexec.args="localhost:15991"
