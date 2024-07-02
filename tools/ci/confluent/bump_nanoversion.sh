#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -euo pipefail

REMOTE="apache"

git remote add $REMOTE git@github.com:apache/flink.git || echo "$REMOTE remote already exists"
git fetch $REMOTE 'refs/tags/*:refs/tags/*'

# To start we want the base version without the snapshot (e.g. 1.17)
BASE_VERSION="$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout | cut -d'-' -f 1)"

# We will be using the latest apache release tag for our base version (e.g. 1.17.0)
VERSION=$(git tag -l "release-${BASE_VERSION}*" | grep -v "\-rc" | sort -V -r | head -n1 | cut -d'-' -f 2)

if [ -n "$VERSION" ]; then
  echo "SKIP Setting base version $VERSION based on most recent upstream tag"
  # disabled as a temporary fix for https://confluentinc.atlassian.net/browse/DP-14979
  # echo "Setting base version $VERSION based on most recent upstream tag"
  # mvn versions:set -DnewVersion="$VERSION" -DgenerateBackupPoms=false
  make mvn-bump-nanoversion
else
  echo "Unable to derive version"
  exit 1
fi
