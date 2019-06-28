#!/bin/bash

# Copyright 2019 Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Enable tty for Windows users using git-bash or cygwin
if [[ "$OSTYPE" == "msys" ]]; then
        # Lightweight shell and GNU utilities compiled for Windows (part of MinGW)
        tty=winpty
        script=//script//fix_replication.sh
fi

# This is a convenience script to fix replication on replicas.
exec $tty docker-compose exec ${CS:-vttablet2} ${script:-/script/fix_replication.sh} "$@"