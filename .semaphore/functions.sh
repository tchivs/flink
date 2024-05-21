#!/usr/bin/env bash
# shellcheck shell=bash

# Publish results into semaphore
publish_test_results() {
  local name=${1:?Missing name}
  local reports_dir
  reports_dir=$(mktemp -d)
  find . -name 'TEST-*Test.xml' -exec cp {} ${reports_dir} \;
  test-results publish "$reports_dir" --name "${name}"
  rm -rf "${reports_dir}"
}
