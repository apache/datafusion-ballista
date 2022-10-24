// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.arrow

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.collection.mutable.ListBuffer
import scala.io.Source

class Conf(args: Array[String]) extends ScallopConf(args) {
  val convertTpch = new Subcommand("convert-tpch") {
    val inputPath = opt[String](required = true)
    val inputFormat = opt[String](required = true)
    val outputPath = opt[String](required = true)
    val outputFormat = opt[String](required = true)
    val partitions = opt[Int](required = true)
  }
  val tpch = new Subcommand("tpch") {
    val inputPath = opt[String](required = true)
    val queryPath = opt[String](required = true)
    val inputFormat = opt[String](required = true)
    val query = opt[String](required = true)
    val iterations = opt[Int](required = false, default = Some(1))
  }
  addSubcommand(convertTpch)
  addSubcommand(tpch)
  requireSubcommand()
  verify()
}

object SparkTpch {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val spark: SparkSession = SparkSession.builder
      .appName("Ballista Spark Benchmarks")
      .getOrCreate()

    conf.subcommand match {
      case Some(conf.tpch) =>
        // register tables
        for (table <- Tpch.tables) {
          val df = readTable(
            conf,
            spark,
            table,
            conf.tpch.inputPath(),
            conf.tpch.inputFormat()
          )
          df.createTempView(table)
        }

        val sqlFile = s"${conf.tpch.queryPath()}/q${conf.tpch.query()}.sql"
        val source = Source.fromFile(sqlFile)
        val sql = source.getLines.mkString("\n")
        source.close()
        println(sql)

        val timing = new ListBuffer[Long]()
        for (i <- 0 until conf.tpch.iterations()) {
          println(s"Iteration $i")
          val start = System.currentTimeMillis()
          val resultDf = spark.sql(sql)
          val results = resultDf.collect()

          val duration = System.currentTimeMillis() - start
          // show results
          println(s"Returned ${results.size} rows:")
          results.foreach(println)

          resultDf.explain(true)
          println(s"Iteration $i took $duration ms")
          timing += duration
        }

        // summarize the results
        timing.zipWithIndex.foreach {
          case (n, i) => println(s"Iteration $i took $n ms")
        }

      case Some(conf.`convertTpch`) =>
        for (table <- Tpch.tables) {
          val df = readTable(
            conf,
            spark,
            table,
            conf.convertTpch.inputPath(),
            conf.convertTpch.inputFormat()
          )

          conf.convertTpch.outputFormat() match {
            case "parquet" =>
              val path = s"${conf.convertTpch.outputPath()}/${table}"
              df.repartition(conf.convertTpch.partitions())
                .write
                .mode(SaveMode.Overwrite)
                .parquet(path)
            case "csv" =>
              val path = s"${conf.convertTpch.outputPath()}/${table}.csv"
              df.repartition(conf.convertTpch.partitions())
                .write
                .mode(SaveMode.Overwrite)
                .csv(path)
            case _ =>
              throw new IllegalArgumentException("unsupported output format")
          }
        }

      case _ =>
        throw new IllegalArgumentException("no subcommand specified")
    }
  }

  private def readTable(
      conf: Conf,
      spark: SparkSession,
      tableName: String,
      inputPath: String,
      inputFormat: String
  ): DataFrame = {
    inputFormat match {
      case "tbl" =>
        val path = s"${inputPath}/${tableName}.tbl"
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "|")
          .schema(Tpch.tableSchema(tableName))
          .csv(path)
      case "csv" =>
        val path = s"${inputPath}/${tableName}.csv"
        spark.read
          .option("header", "false")
          .option("inferSchema", "false")
          .schema(Tpch.tableSchema(tableName))
          .csv(path)
      case "parquet" =>
        val path = s"${inputPath}/${tableName}"
        spark.read
          .parquet(path)
      case _ =>
        throw new IllegalArgumentException("unsupported input format")
    }
  }
}

object Tpch {

  val tables = Seq(
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
    "nation",
    "region"
  )

  def tableSchema(tableName: String) = {

    // TODO should be using DecimalType here
    //val decimalType = DataTypes.createDecimalType(15, 2)
    val decimalType = DataTypes.DoubleType

    tableName match {
      case "part" => new StructType(Array(
            StructField("p_partkey", DataTypes.LongType, false),
            StructField("p_name", DataTypes.StringType, false),
            StructField("p_mfgr", DataTypes.StringType, false),
            StructField("p_brand", DataTypes.StringType, false),
            StructField("p_type", DataTypes.StringType, false),
            StructField("p_size", DataTypes.IntegerType, false),
            StructField("p_container", DataTypes.StringType, false),
            StructField("p_retailprice", decimalType, false),
            StructField("p_comment", DataTypes.StringType, false)
      ))

      case "supplier" => new StructType(Array(
          StructField("s_suppkey", DataTypes.LongType, false),
          StructField("s_name", DataTypes.StringType, false),
          StructField("s_address", DataTypes.StringType, false),
          StructField("s_nationkey", DataTypes.LongType, false),
          StructField("s_phone", DataTypes.StringType, false),
          StructField("s_acctbal", decimalType, false),
          StructField("s_comment", DataTypes.StringType, false)
      ))

      case "partsupp" => new StructType(Array(
          StructField("ps_partkey", DataTypes.LongType, false),
          StructField("ps_suppkey", DataTypes.LongType, false),
          StructField("ps_availqty", DataTypes.IntegerType, false),
          StructField("ps_supplycost", decimalType, false),
          StructField("ps_comment", DataTypes.StringType, false)
      ))

      case "customer" => new StructType(Array(
          StructField("c_custkey", DataTypes.LongType, false),
          StructField("c_name", DataTypes.StringType, false),
          StructField("c_address", DataTypes.StringType, false),
          StructField("c_nationkey", DataTypes.LongType, false),
          StructField("c_phone", DataTypes.StringType, false),
          StructField("c_acctbal", decimalType, false),
          StructField("c_mktsegment", DataTypes.StringType, false),
          StructField("c_comment", DataTypes.StringType, false)
      ))

      case "orders" => new StructType(Array(
          StructField("o_orderkey", DataTypes.LongType, false),
          StructField("o_custkey", DataTypes.LongType, false),
          StructField("o_orderstatus", DataTypes.StringType, false),
          StructField("o_totalprice", decimalType, false),
          StructField("o_orderdate", DataTypes.DateType, false),
          StructField("o_orderpriority", DataTypes.StringType, false),
          StructField("o_clerk", DataTypes.StringType, false),
          StructField("o_shippriority", DataTypes.IntegerType, false),
          StructField("o_comment", DataTypes.StringType, false)
      ))

      case "lineitem" => new StructType(Array(
          StructField("l_orderkey", DataTypes.LongType, false),
          StructField("l_partkey", DataTypes.LongType, false),
          StructField("l_suppkey", DataTypes.LongType, false),
          StructField("l_linenumber", DataTypes.IntegerType, false),
          StructField("l_quantity", decimalType, false),
          StructField("l_extendedprice", decimalType, false),
          StructField("l_discount", decimalType, false),
          StructField("l_tax", decimalType, false),
          StructField("l_returnflag", DataTypes.StringType, false),
          StructField("l_linestatus", DataTypes.StringType, false),
          StructField("l_shipdate", DataTypes.DateType, false),
          StructField("l_commitdate", DataTypes.DateType, false),
          StructField("l_receiptdate", DataTypes.DateType, false),
          StructField("l_shipinstruct", DataTypes.StringType, false),
          StructField("l_shipmode", DataTypes.StringType, false),
          StructField("l_comment", DataTypes.StringType, false)
      ))

      case "nation" => new StructType(Array(
          StructField("n_nationkey", DataTypes.LongType, false),
          StructField("n_name", DataTypes.StringType, false),
          StructField("n_regionkey", DataTypes.LongType, false),
          StructField("n_comment", DataTypes.StringType, false)
      ))

      case "region" => new StructType(Array(
          StructField("r_regionkey", DataTypes.LongType, false),
          StructField("r_name", DataTypes.StringType, false),
          StructField("r_comment", DataTypes.StringType, false)
      ))

      case other => throw new UnsupportedOperationException(tableName)
    }
  }
}
