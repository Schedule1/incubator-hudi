/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.uber.hoodie.common.HoodieTestDataGenerator
import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.junit.Assert._
import org.junit.rules.TemporaryFolder
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Basic tests on the spark datasource
  */
class DataSourceTest extends AssertionsForJUnit {

  var spark: SparkSession = _
  var dataGen: HoodieTestDataGenerator = _
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )
  var basePath: String = _
  var fs: FileSystem = _

  @Before def initialize() {
    spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate
    dataGen = new HoodieTestDataGenerator()
    val folder = new TemporaryFolder
    folder.create()
    basePath = folder.getRoot.getAbsolutePath
    fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
  }

  @Test def testInsertBothOldAndNew(): Unit = {

    // Insert Operation
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    def validateBothViews(n: Int): Unit = {

      // Read RO View TODO: always need to specify /*/* ... ?
      // TODO: this looks like a flawed design, the the number of /* depends on content of the table
      // as a result it is useless in general case, incremental view type must be enabled at all time.
      val hoodieROViewDF = spark.read.format("com.uber.hoodie")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_READ_OPTIMIZED_OPT_VAL)
        .schema(inputDF1.schema)
        .load(basePath + "/*/*/*/*")
      assertEquals(inputDF1.schema.fieldNames.seq, hoodieROViewDF.schema.fieldNames.seq)
      assertEquals(n, hoodieROViewDF.count())

      val hoodieIncViewDF = spark.read.format("com.uber.hoodie")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
        .schema(inputDF1.schema)
        .load(basePath)
//      assertEquals(inputDF1.schema.fieldNames.seq, hoodieIncViewDF.schema.fieldNames.seq) TODO: how to comply?
      assertEquals(n, hoodieIncViewDF.count())
    }

    validateBothViews(100)

    val oldRecords = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("001", 100)).toList
    val oldDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(oldRecords, 2))

    val newRecords = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 20)).toList
    val newDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(newRecords, 2))

    val inputDF2 = oldDF.union(newDF)

    val uniqueKeyCnt = oldDF.select("_row_key").distinct().count()

    val inputKeyStrs = Seq(inputDF1, inputDF2).map {
      df =>
        df.select("_row_key").distinct().rdd.map(_.getAs[String](0)).collect().sorted.mkString("\n")
    }

    // Upsert Operation
    inputDF2.write.format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    validateBothViews(120)

    //    val hoodieRTViewDF2 = spark.read.format("com.uber.hoodie")
    //      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_REALTIME_OPT_VAL)
    ////      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
    //      .load(basePath)
    //    assertEquals(120, hoodieRTViewDF2.count())

    // Read Incremental View
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    val hoodieIncViewDF1 = spark.read.format("com.uber.hoodie")
      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
      .load(basePath)
    assertEquals(100, hoodieIncViewDF1.count())

    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("com.uber.hoodie")
      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(uniqueKeyCnt + 20, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
  }

  @Test def testInsertEmpty(): Unit = {

    import functions._

    val _basePath = basePath + "/empty"

    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    val inputEmpty = inputDF1.filter(lit(1) === lit(0))
    inputEmpty.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(_basePath)

    val read = spark.read.format("com.uber.hoodie")
      .schema(inputEmpty.schema)

    // Read RO View
    def hoodieROViewDF1 = read
      .load(_basePath + "/*/*/*/*")

    //    val hoodieROViewDF1 = read
    //      .load(basePath)

    assertEquals(0, hoodieROViewDF1.count())

    // Upsert Operation
    inputDF1.write.format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(_basePath)

    assertEquals(100, hoodieROViewDF1.count())
  }

  @Test def testCopyOnWriteStorage() {
    // Insert Operation
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read.format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hoodieROViewDF1.count())

    val records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("001", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    // Upsert Operation
    inputDF2.write.format("com.uber.hoodie")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read.format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*")
    assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated

    // Read Incremental View
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0)
    val hoodieIncViewDF1 = spark.read.format("com.uber.hoodie")
      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
      .load(basePath)
    assertEquals(100, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("com.uber.hoodie")
      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
  }

  @Test def testMergeOnReadStorage() {
    // Bulk Insert Operation
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_STORAGE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Read RO View
    val hoodieROViewDF1 = spark.read.format("com.uber.hoodie").load(basePath + "/*/*/*/*")
    assertEquals(100, hoodieROViewDF1.count()) // still 100, since we only updated
  }

  @Test def testDropInsertDup(): Unit = {
    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords =  dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2New = dataGen.generateSameKeyInserts("002", allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts("002", inserts1.subList(0, insert2DupKeyCnt))

    val records1 = DataSourceTestUtils.convertToStringList(inserts1).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*")
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val records2 = DataSourceTestUtils
      .convertToStringList(inserts2Dup ++ inserts2New)
      .toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("com.uber.hoodie")
      .options(commonOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("com.uber.hoodie")
      .load(basePath + "/*/*/*/*")
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("com.uber.hoodie")
      .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  //@Test (TODO: re-enable after fixing noisyness)
  def testStructuredStreaming(): Unit = {
    fs.delete(new Path(basePath), true)
    val sourcePath = basePath + "/source"
    val destPath = basePath + "/dest"
    fs.mkdirs(new Path(sourcePath))

    // First chunk of data
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second chunk of data
    val records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("001", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    // define the source of streaming
    val streamingInput =
      spark.readStream
        .schema(inputDF1.schema)
        .json(sourcePath)

    val f1 = Future {
      println("streaming starting")
      //'writeStream' can be called only on streaming Dataset/DataFrame
      streamingInput
        .writeStream
        .format("com.uber.hoodie")
        .options(commonOpts)
        .trigger(new ProcessingTime(100))
        .option("checkpointLocation", basePath + "/checkpoint")
        .outputMode(OutputMode.Append)
        .start(destPath)
        .awaitTermination(10000)
      println("streaming ends")
    }

    val f2 = Future {
      inputDF1.write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      Thread.sleep(3000)
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, destPath, "000"))
      val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, destPath)
      // Read RO View
      val hoodieROViewDF1 = spark.read.format("com.uber.hoodie")
        .load(destPath + "/*/*/*/*")
      assert(hoodieROViewDF1.count() == 100)

      inputDF2.write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      Thread.sleep(3000)
      val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, destPath)
      assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").size())
      // Read RO View
      val hoodieROViewDF2 = spark.read.format("com.uber.hoodie")
        .load(destPath + "/*/*/*/*")
      assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated


      // Read Incremental View
      // we have 2 commits, try pulling the first commit (which is not the latest)
      val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").get(0)
      val hoodieIncViewDF1 = spark.read.format("com.uber.hoodie")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
        .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
        .load(destPath)
      assertEquals(100, hoodieIncViewDF1.count())
      // 100 initial inserts must be pulled
      var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(firstCommit, countsPerCommit(0).get(0))

      // pull the latest commit
      val hoodieIncViewDF2 = spark.read.format("com.uber.hoodie")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
        .load(destPath)

      assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
      countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
    }

    Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)

  }
}
