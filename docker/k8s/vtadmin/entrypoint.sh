#!/bin/bash

# Copyright 2022 The Vitess Authors.
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

set -e

content=$(cat /vt/web/vtadmin/build/index.html)
content=$(echo $content | sed "s|#REACT_APP_VTADMIN_API_ADDRESS#|$REACT_APP_VTADMIN_API_ADDRESS|")
content=$(echo $content | sed "s|#REACT_APP_FETCH_CREDENTIALS#|${REACT_APP_FETCH_CREDENTIALS:-}|")
content=$(echo $content | sed "s|#REACT_APP_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS#|${REACT_APP_ENABLE_EXPERIMENTAL_TABLET_DEBUG_VARS:-true}|")
content=$(echo $content | sed "s|#REACT_APP_BUGSNAG_API_KEY#|${REACT_APP_BUGSNAG_API_KEY:-}|")
content=$(echo $content | sed "s|#REACT_APP_DOCUMENT_TITLE#|${REACT_APP_DOCUMENT_TITLE:-}|")
content=$(echo $content | sed "s|#REACT_APP_READONLY_MODE#|${REACT_APP_READONLY_MODE:-false}|")
echo "$content" > /vt/web/vtadmin/build/index.html

exec /bin/bash -c "$*"