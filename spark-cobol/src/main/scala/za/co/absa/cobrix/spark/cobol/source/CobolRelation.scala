/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.cobrix.spark.cobol.source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import za.co.absa.cobrix.cobol.reader.parameters.VsamParameters
import za.co.absa.cobrix.cobol.reader.index.entry.SparseIndexEntry
import za.co.absa.cobrix.spark.cobol.reader.{FixedLenReader, FixedLenTextReader, Reader, VarLenReader}
import za.co.absa.cobrix.spark.cobol.parameters.MetadataFields.{VSAM_RBA, VSAM_RRN}
import za.co.absa.cobrix.spark.cobol.source.index.IndexBuilder
import za.co.absa.cobrix.spark.cobol.source.parameters.LocalityParameters
import za.co.absa.cobrix.spark.cobol.source.scanners.CobolScanners
import za.co.absa.cobrix.spark.cobol.source.types.FileWithOrder
import za.co.absa.cobrix.spark.cobol.source.vsam.{JZosVsamSource, VsamDataset, VsamFilterPushdown, VsamSource}
import za.co.absa.cobrix.spark.cobol.utils.FileUtils

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.math.{BigDecimal => JBigDecimal}
import scala.util.control.NonFatal


class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit =
    try {
      out.defaultWriteObject()
      value.write(out)
    } catch {
      case NonFatal(e) =>
        throw new IOException(e)
    }

  private def readObject(in: ObjectInputStream): Unit =
    try {
      value = new Configuration(false)
      value.readFields(in)
    } catch {
      case NonFatal(e) =>
        throw new IOException(e)
    }
}

/**
  * This class implements an actual Spark relation.
  *
  * It currently supports both, fixed and variable-length records.
  *
  * Its constructor is expected to change after the hierarchy of [[za.co.absa.cobrix.spark.cobol.reader.Reader]] is put in place.
  */
class CobolRelation(sourceDirs: Seq[String],
                    filesList: Array[FileWithOrder],
                    cobolReader: Reader,
                    localityParams: LocalityParameters,
                    debugIgnoreFileSize: Boolean,
                    vsamParams: Option[VsamParameters],
                    vsamSourceProvider: Int => VsamSource = recordSize => new JZosVsamSource(recordSize))
                   (@transient val sqlContext: SQLContext)
  extends BaseRelation
    with Serializable
    with PrunedFilteredScan {

  private lazy val indexes: RDD[SparseIndexEntry] = IndexBuilder.buildIndex(filesList, cobolReader, sqlContext, cobolReader.getReaderProperties.isIndexCachingAllowed)(localityParams)
  private lazy val baseSchema: StructType = cobolReader.getSparkSchema
  private lazy val relationSchema: StructType = vsamParams match {
    case Some(params) =>
      params.organization.entryName match {
        case "esds" => baseSchema.add(StructField(VSAM_RBA, LongType, nullable = false))
        case "rrds" => baseSchema.add(StructField(VSAM_RRN, LongType, nullable = false))
        case _ => baseSchema
      }
    case None => baseSchema
  }

  override def schema: StructType = {
    relationSchema
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val fullScan = vsamParams match {
      case Some(params) => buildVsamScan(filters, params)
      case None => buildFileScan()
    }

    projectRows(fullScan, requiredColumns)
  }

  private[source] def parseRecords(reader: FixedLenReader, records: RDD[Array[Byte]]): RDD[Row] = {
    records.flatMap(record => {
      val it = reader.getRowIterator(record)
      for (parsedRecord <- it) yield {
        parsedRecord
      }
    })
  }

  private def buildFileScan(): RDD[Row] = {
    cobolReader match {
      case blockReader: FixedLenTextReader =>
        CobolScanners.buildScanForTextFiles(blockReader, sourceDirs, parseRecords, sqlContext)
      case blockReader: FixedLenReader =>
        CobolScanners.buildScanForFixedLength(blockReader, sourceDirs, parseRecords, debugIgnoreFileSize, sqlContext)
      case streamReader: VarLenReader if streamReader.isIndexGenerationNeeded =>
        CobolScanners.buildScanForVarLenIndex(streamReader, indexes, filesList, sqlContext)
      case streamReader: VarLenReader =>
        CobolScanners.buildScanForVariableLength(streamReader, filesList, sqlContext)
      case _ =>
        throw new IllegalStateException(s"Invalid reader object $cobolReader.")
    }
  }

  private def buildVsamScan(filters: Array[Filter], params: VsamParameters): RDD[Row] = {
    val fixedLenReader = cobolReader match {
      case reader: FixedLenReader => reader
      case _ => throw new IllegalArgumentException("VSAM source requires a fixed-length reader.")
    }

    val scanPlan = VsamFilterPushdown.plan(filters, params, fixedLenReader.getCobolSchema.copybook)
    val datasets = sourceDirs.map(VsamDataset.fromPath)
    val datasetRDD = sqlContext.sparkContext.parallelize(datasets, datasets.size)

    val scannedRows = datasetRDD.mapPartitions { partition =>
      val vsamSource = vsamSourceProvider(fixedLenReader.getRecordSize)
      partition.flatMap { dataset =>
        vsamSource.scan(dataset, params.organization, scanPlan.accessPlan).flatMap { record =>
          val parsed = fixedLenReader.getRowIterator(record.bytes)
          parsed.map(row => appendVsamMetadata(row, record))
        }
      }
    }

    if (scanPlan.residualFilters.nonEmpty) {
      scannedRows.filter(row => scanPlan.residualFilters.forall(matchesFilter(row, relationSchema, _)))
    } else {
      scannedRows
    }
  }

  private def appendVsamMetadata(row: Row, record: za.co.absa.cobrix.spark.cobol.source.vsam.VsamRecord): Row = {
    if (relationSchema == baseSchema) {
      row
    } else {
      val extraValues = record.rba.orElse(record.rrn).toSeq
      new GenericRowWithSchema((row.toSeq ++ extraValues).toArray, relationSchema)
    }
  }

  private def projectRows(records: RDD[Row], requiredColumns: Array[String]): RDD[Row] = {
    if (requiredColumns.sameElements(schema.fieldNames)) {
      records
    } else {
      val requiredSchema = StructType(requiredColumns.map(column => schema(schema.fieldIndex(column))))
      val indexes = requiredColumns.map(schema.fieldIndex)

      records.map { row =>
        new GenericRowWithSchema(indexes.map(row.get), requiredSchema)
      }
    }
  }

  private def matchesFilter(row: Row, rowSchema: StructType, filter: Filter): Boolean = filter match {
    case EqualTo(attribute, value) => compareColumn(row, rowSchema, attribute, value)(_ == 0)
    case EqualNullSafe(attribute, value) =>
      val fieldValue = getFieldValue(row, rowSchema, attribute)
      if (fieldValue == null || value == null) {
        fieldValue == null && value == null
      } else {
        compareValues(fieldValue, value) == 0
      }
    case GreaterThan(attribute, value) => compareColumn(row, rowSchema, attribute, value)(_ > 0)
    case GreaterThanOrEqual(attribute, value) => compareColumn(row, rowSchema, attribute, value)(_ >= 0)
    case LessThan(attribute, value) => compareColumn(row, rowSchema, attribute, value)(_ < 0)
    case LessThanOrEqual(attribute, value) => compareColumn(row, rowSchema, attribute, value)(_ <= 0)
    case In(attribute, values) =>
      val fieldValue = getFieldValue(row, rowSchema, attribute)
      fieldValue != null && values.exists(value => compareValues(fieldValue, value) == 0)
    case IsNull(attribute) => getFieldValue(row, rowSchema, attribute) == null
    case IsNotNull(attribute) => getFieldValue(row, rowSchema, attribute) != null
    case StringStartsWith(attribute, value) => stringColumn(row, rowSchema, attribute).exists(_.startsWith(value))
    case StringEndsWith(attribute, value) => stringColumn(row, rowSchema, attribute).exists(_.endsWith(value))
    case StringContains(attribute, value) => stringColumn(row, rowSchema, attribute).exists(_.contains(value))
    case And(left, right) => matchesFilter(row, rowSchema, left) && matchesFilter(row, rowSchema, right)
    case Or(left, right) => matchesFilter(row, rowSchema, left) || matchesFilter(row, rowSchema, right)
    case Not(child) => !matchesFilter(row, rowSchema, child)
    case _ => true
  }

  private def compareColumn(row: Row, rowSchema: StructType, attribute: String, value: Any)(predicate: Int => Boolean): Boolean = {
    val fieldValue = getFieldValue(row, rowSchema, attribute)
    fieldValue != null && value != null && predicate(compareValues(fieldValue, value))
  }

  private def getFieldValue(row: Row, rowSchema: StructType, attribute: String): Any = {
    val fieldIndex = rowSchema.fieldNames.indexWhere(_.equalsIgnoreCase(attribute))
    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) row.get(fieldIndex) else null
  }

  private def stringColumn(row: Row, rowSchema: StructType, attribute: String): Option[String] =
    Option(getFieldValue(row, rowSchema, attribute)).map(_.toString)

  private def compareValues(left: Any, right: Any): Int = {
    (left, right) match {
      case (leftNumber: java.lang.Number, rightNumber: java.lang.Number) =>
        BigDecimal(new JBigDecimal(leftNumber.toString)).compare(BigDecimal(new JBigDecimal(rightNumber.toString)))
      case (leftBoolean: java.lang.Boolean, rightBoolean: java.lang.Boolean) =>
        leftBoolean.compareTo(rightBoolean)
      case _ =>
        left.toString.compareTo(right.toString)
    }
  }
}

object CobolRelation {
  /**
    * Retrieves a list containing the files contained in the directory to be processed attached to numbers which serve
    * as their order.
    *
    * The List contains [[za.co.absa.cobrix.spark.cobol.source.types.FileWithOrder]] instances.
    */
  def getListFilesWithOrder(sourceDirs: Seq[String], sqlContext: SQLContext, isRecursiveRetrieval: Boolean): Array[FileWithOrder] = {
    val allFiles = sourceDirs.flatMap(sourceDir => {
      FileUtils
        .getFiles(sourceDir, sqlContext.sparkContext.hadoopConfiguration, isRecursiveRetrieval)
    }).toArray

    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val factory = new CompressionCodecFactory(hadoopConf)

    allFiles
      .zipWithIndex
      .map { case (fileName, order) =>
        val codec = factory.getCodec(new Path(fileName))
        val isCompressed = codec != null

        FileWithOrder(fileName, order, isCompressed)
      }
  }
}
