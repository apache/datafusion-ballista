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

# Spark benchmark image: the official Apache Spark image (with Python, for
# spark/tpcbench.py) plus the S3A connector, which it does not include.
#
#   docker build -f spark.Dockerfile -t <registry>/spark-bench:4.1.3 .
#
# The hadoop-aws version must be the Hadoop version the Spark release bundles
# (ls /opt/spark/jars/hadoop-client-*), and the AWS SDK bundle and S3
# Analytics Accelerator the versions that hadoop-aws release depends on. The
# defaults are for Spark 4.1.3, which bundles Hadoop 3.4.2. Before Hadoop
# 3.4.2 there is no Analytics Accelerator: remove its ADD.

ARG SPARK_IMAGE=apache/spark:4.1.3
FROM ${SPARK_IMAGE}

ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
ARG ANALYTICS_ACCELERATOR_VERSION=1.2.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/${ANALYTICS_ACCELERATOR_VERSION}/analyticsaccelerator-s3-${ANALYTICS_ACCELERATOR_VERSION}.jar /opt/spark/jars/
USER spark
