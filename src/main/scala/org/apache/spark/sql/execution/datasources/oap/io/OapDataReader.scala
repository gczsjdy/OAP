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

package org.apache.spark.sql.execution.datasources.oap.io

import java.net.URI

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.hadoop.util.StringUtils

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{OapException, PartitionedFile}
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache.DataFileMetaCacheManager
import org.apache.spark.sql.execution.datasources.oap.index.{IndexContext, ScannerBuilder}
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion
import org.apache.spark.sql.execution.datasources.oap.io.OapDataFileProperties.DataFileVersion._
import org.apache.spark.sql.execution.datasources.oap.utils.FilterHelper
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

object OapDataReader extends Logging {

  def read(meta: Option[DataSourceMeta],
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hitIndexColumns: mutable.Map[String, IndexType],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO we need to pass the extra data source meta information via the func parameter
    meta match {
      case Some(m) =>
        logDebug("Building OapDataReader with "
            + m.dataReaderClassName.substring(m.dataReaderClassName.lastIndexOf(".") + 1)
            + " ...")

        // Check whether this filter conforms to certain patterns that could benefit from index
        def canTriggerIndex(filter: Filter): Boolean = {
          var attr: String = null
          def checkAttribute(filter: Filter): Boolean = filter match {
            case Or(left, right) =>
              checkAttribute(left) && checkAttribute(right)
            case And(left, right) =>
              checkAttribute(left) && checkAttribute(right)
            case EqualTo(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case LessThan(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case LessThanOrEqual(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case GreaterThan(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case GreaterThanOrEqual(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case In(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case IsNull(attribute) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case IsNotNull(attribute) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case StringStartsWith(attribute, _) =>
              if (attr == null || attr == attribute) {
                attr = attribute; true
              } else false
            case _ => false
          }

          checkAttribute(filter)
        }

        val ic = new IndexContext(m)

        if (m.indexMetas.nonEmpty) {
          // check and use index
          logDebug("Supported Filters by Oap:")
          // filter out the "filters" on which we can use index
          val supportFilters = filters.toArray.filter(canTriggerIndex)
          // After filtered, supportFilter only contains:
          // 1. Or predicate that contains only one attribute internally;
          // 2. Some atomic predicates, such as LessThan, EqualTo, etc.
          if (supportFilters.nonEmpty) {
            // determine whether we can use index
            supportFilters.foreach(filter => logDebug("\t" + filter.toString))
            // get index options such as limit, order, etc.
            val indexOptions = options.filterKeys(OapFileFormat.oapOptimizationKeySeq.contains(_))
            val maxChooseSize = sparkSession.conf.get(OapConf.OAP_INDEXER_CHOICE_MAX_SIZE)
            val indexDisableList = sparkSession.conf.get(OapConf.OAP_INDEX_DISABLE_LIST)
            ScannerBuilder.build(supportFilters, ic, indexOptions, maxChooseSize, indexDisableList)
          }
        }

        val filterScanners = ic.getScanners
        filterScanners match {
          case Some(s) =>
            hitIndexColumns ++= s.scanners.flatMap { scanner =>
              scanner.keyNames.map(n => n -> scanner.meta.indexType)
            }.toMap
          case _ =>
        }

        val requiredIds = requiredSchema.map(dataSchema.fields.indexOf(_)).toArray
        val pushed = FilterHelper.tryToPushFilters(sparkSession, requiredSchema, filters)

        // Refer to ParquetFileFormat, use resultSchema to decide if this query support
        // Vectorized Read and returningBatch. Also it depends on WHOLE_STAGE_CODE_GEN,
        // as the essential unsafe projection is done by that.
        val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
        val enableVectorizedReader: Boolean =
          m.dataReaderClassName.equals(OapFileFormat.PARQUET_DATA_FILE_CLASSNAME) &&
              sparkSession.sessionState.conf.parquetVectorizedReaderEnabled &&
              sparkSession.sessionState.conf.wholeStageEnabled &&
              resultSchema.forall(_.dataType.isInstanceOf[AtomicType])
        val returningBatch = supportBatch(sparkSession, resultSchema)
        val parquetDataCacheEnable = sparkSession.conf.get(OapConf.OAP_PARQUET_DATA_CACHE_ENABLED)
        val broadcastedHadoopConf =
          sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

        (file: PartitionedFile) => {

          val path = new Path(StringUtils.unEscapeString(file.filePath))
          val fs = path.getFileSystem(broadcastedHadoopConf.value.value)
          val version = readVersion(fs.open(path), fs.getFileStatus(path).getLen)
          version match {
            case DataFileVersion.OAP_DATAFILE_V1 => new OapDataReaderV1().read()
          }

          assert(file.partitionValues.numFields == partitionSchema.size)
          val conf = broadcastedHadoopConf.value.value

          def isSkippedByFile: Boolean = {
            if (m.dataReaderClassName == OapFileFormat.OAP_DATA_FILE_CLASSNAME) {
              val dataFile = DataFile(file.filePath, m.schema, m.dataReaderClassName, conf)
              val dataFileMeta = DataFileMetaCacheManager(dataFile)
                  .asInstanceOf[OapDataFileMetaV1]
              if (filters.exists(filter => isSkippedByStatistics(
                dataFileMeta.columnsMeta.map(_.fileStatistics).toArray, filter, m.schema))) {
                val totalRows = dataFileMeta.totalRowCount()
                oapMetrics.updateTotalRows(totalRows)
                oapMetrics.skipForStatistic(totalRows)
                return true
              }
            }
            false
          }

          if (isSkippedByFile) {
            Iterator.empty
          } else {
            OapIndexInfo.partitionOapIndex.put(file.filePath, false)
            FilterHelper.setFilterIfExist(conf, pushed)
            // if enableVectorizedReader == true, init VectorizedContext,
            // else context is None.
            val context = if (enableVectorizedReader) {
              Some(VectorizedContext(partitionSchema, file.partitionValues, returningBatch))
            } else {
              None
            }
            val reader = new OapDataScannerV1(
              new Path(new URI(file.filePath)), m, filterScanners, requiredIds, context)
            val iter = reader.initialize(conf, options, filters)
            Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
            val totalRows = reader.totalRows()
            oapMetrics.updateTotalRows(totalRows)
            oapMetrics.updateIndexAndRowRead(reader, totalRows)
            // if enableVectorizedReader == true and parquetDataCacheEnable = false,
            // return iter directly because of partitionValues
            // already filled by VectorizedReader, else use original branch.
            if (enableVectorizedReader && !parquetDataCacheEnable) {
              iter
            } else {
              val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
              val joinedRow = new JoinedRow()
              val appendPartitionColumns =
                GenerateUnsafeProjection.generate(fullSchema, fullSchema)

              iter.map(d => appendPartitionColumns(joinedRow(d, file.partitionValues)))
            }
          }
        }
      case None => (_: PartitionedFile) => {
        // TODO need to think about when there is no oap.meta file at all
        Iterator.empty
      }
    }
  }



  def readVersion(is: FSDataInputStream, fileLen: Long): DataFileVersion = {
    val MAGIC_VERSION_LENGTH = 4
    val oapDataFileMetaLengthIndex = fileLen - 4

    // seek to the position of data file meta length
    is.seek(oapDataFileMetaLengthIndex)
    val oapDataFileMetaLength = is.readInt()
    // read all bytes of data file meta
    val magicBuffer = new Array[Byte](MAGIC_VERSION_LENGTH)
    is.readFully(oapDataFileMetaLengthIndex - oapDataFileMetaLength, magicBuffer)

    val magic = UTF8String.fromBytes(magicBuffer).toString
    if (! magic.contains("OAP")) {
      throw new OapException("Not a valid Oap Data File")
    } else if (magic == "OAP1") {
      DataFileVersion.OAP_DATAFILE_V1
    } else {
      throw new OapException("Not a supported Oap Data File version")
    }
  }

}

abstract class OapDataReader {
  def read(file: PartitionedFile): Iterator[InternalRow]
}

class OapDataReaderV1 extends OapDataReader {

}
