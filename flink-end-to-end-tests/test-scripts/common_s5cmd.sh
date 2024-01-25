#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

if [[ -z "$S3_SOURCED:-" ]]; then
  echo "S3 not configured" && exit 1
fi

function s5cmd_setup {
  #sudo apt-get install -y golang-go
  #go install github.com/peak/s5cmd/v2@v2.2.0
  retry_download https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz
  sudo tar xvf /tmp/s5cmd_2.2.2_Linux-64bit.tar.gz -C /tmp
  set_config_key "s3.s5cmd.path" "/tmp/s5cmd"
}
