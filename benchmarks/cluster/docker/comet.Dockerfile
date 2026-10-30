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

# Spark + Comet benchmark image: the Spark image from spark.Dockerfile with the
# Comet JAR in /opt/spark/jars, so it is on the driver and executor classpath.
#
#   docker build -f comet.Dockerfile \
#     --build-arg SPARK_IMAGE=<registry>/spark-bench:4.1.3 \
#     --build-arg COMET_JAR_URL=https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/<version>/comet-spark-spark4.1_2.13-<version>.jar \
#     -t <registry>/comet-bench:4.1.3-<version> .
#
# The JAR must match the Spark and Scala versions of the image. To use a JAR
# you built, serve it over HTTP or replace the ADD with a COPY.

ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

ARG COMET_JAR_URL

USER root
ADD --chmod=644 ${COMET_JAR_URL} /opt/spark/jars/
USER spark
