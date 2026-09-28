#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

RAT_VERSION=0.13

# download apache rat
if [ ! -f apache-rat-${RAT_VERSION}.jar ]; then
  # -f so an HTTP error fails here instead of caching an error page as the jar.
  if ! curl -sSfL -o apache-rat-${RAT_VERSION}.jar https://repo1.maven.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar; then
    rm -f apache-rat-${RAT_VERSION}.jar
    echo "Failed to download apache-rat-${RAT_VERSION}.jar"
    exit 1
  fi
fi

RAT="java -jar apache-rat-${RAT_VERSION}.jar -x "

RELEASE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

# generate the rat report
if ! $RAT $1 > rat.txt; then
  echo "Apache RAT failed; see the error above"
  exit 1
fi

python3 $RELEASE_DIR/check-rat-report.py $RELEASE_DIR/rat_exclude_files.txt rat.txt > filtered_rat.txt
CHECK_STATUS=$?
cat filtered_rat.txt
UNAPPROVED=`grep -c "NOT APPROVED" filtered_rat.txt`

# A nonzero exit with no NOT APPROVED lines means the checker itself failed,
# e.g. python3 missing or rat.txt not being a valid report.
if [ "${CHECK_STATUS}" -eq 0 ]; then
  echo "No unapproved licenses"
elif [ "${UNAPPROVED}" -gt 0 ]; then
  echo "${UNAPPROVED} unapproved licences. Check rat report: rat.txt"
  exit 1
else
  echo "check-rat-report.py failed (exit ${CHECK_STATUS}); see rat.txt"
  exit 1
fi
