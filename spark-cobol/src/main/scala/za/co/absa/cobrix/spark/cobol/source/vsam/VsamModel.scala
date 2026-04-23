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

package za.co.absa.cobrix.spark.cobol.source.vsam

import org.apache.spark.sql.sources._
import za.co.absa.cobrix.cobol.parser.Copybook
import za.co.absa.cobrix.cobol.parser.ast.Primitive
import za.co.absa.cobrix.cobol.reader.parameters.{VsamOrganization, VsamParameters}
import za.co.absa.cobrix.spark.cobol.parameters.MetadataFields.{VSAM_RBA, VSAM_RRN}

case class VsamDataset(path: String, datasetName: String)

object VsamDataset {
  private val Prefix = "vsam://"

  def fromPath(path: String): VsamDataset = {
    val trimmed = path.trim
    val datasetName = trimmed.stripPrefix(Prefix)

    if (!trimmed.toLowerCase.startsWith(Prefix) || datasetName.isEmpty) {
      throw new IllegalArgumentException(s"Invalid VSAM path '$path'. Expected format: vsam://HLQ.DATASET")
    }

    VsamDataset(trimmed, datasetName)
  }
}

case class VsamRecord(bytes: Array[Byte], rba: Option[Long], rrn: Option[Long])

sealed trait VsamAccessPlan

object VsamAccessPlan {
  case object SequentialScan extends VsamAccessPlan
  case class KeyEquals(keys: Seq[Array[Byte]]) extends VsamAccessPlan
  case class KeyRange(lowerBound: Option[Array[Byte]], upperBound: Option[Array[Byte]]) extends VsamAccessPlan
  case class Positions(values: Seq[Long]) extends VsamAccessPlan
  case class PositionRange(lowerBound: Option[Long], upperBound: Option[Long]) extends VsamAccessPlan
}

case class VsamScanPlan(accessPlan: VsamAccessPlan, pushedFilters: Array[Filter], residualFilters: Array[Filter])

trait VsamSource extends Serializable {
  def scan(dataset: VsamDataset, organization: VsamOrganization, accessPlan: VsamAccessPlan): Iterator[VsamRecord]
}

object VsamFilterPushdown {
  def plan(filters: Array[Filter], vsamParams: VsamParameters, copybook: Copybook): VsamScanPlan = {
    val targetColumn = vsamParams.organization match {
      case VsamOrganization.Ksds => vsamParams.keyColumn.get
      case VsamOrganization.Esds => VSAM_RBA
      case VsamOrganization.Rrds => VSAM_RRN
    }

    val targetField = if (vsamParams.organization == VsamOrganization.Ksds) {
      Some(copybook.getFieldByName(targetColumn).asInstanceOf[Primitive])
    } else {
      None
    }

    val grouped = filters.foldLeft((Vector.empty[Filter], Vector.empty[Filter])) { case ((pushable, residual), filter) =>
      if (isPushable(filter, targetColumn)) {
        (pushable :+ filter, residual)
      } else {
        (pushable, residual :+ filter)
      }
    }

    val pushed = grouped._1.toArray
      val accessPlan = toAccessPlan(pushed, targetColumn, targetField, vsamParams.organization, copybook).getOrElse(VsamAccessPlan.SequentialScan)
    val effectiveResidual = if (accessPlan == VsamAccessPlan.SequentialScan) filters else grouped._2.toArray

    VsamScanPlan(accessPlan, pushed, effectiveResidual)
  }

  private def isPushable(filter: Filter, targetColumn: String): Boolean = filter match {
    case EqualTo(attribute, _) => attribute.equalsIgnoreCase(targetColumn)
    case In(attribute, _) => attribute.equalsIgnoreCase(targetColumn)
    case GreaterThan(attribute, _) => attribute.equalsIgnoreCase(targetColumn)
    case GreaterThanOrEqual(attribute, _) => attribute.equalsIgnoreCase(targetColumn)
    case LessThan(attribute, _) => attribute.equalsIgnoreCase(targetColumn)
    case LessThanOrEqual(attribute, _) => attribute.equalsIgnoreCase(targetColumn)
    case And(left, right) => isPushable(left, targetColumn) && isPushable(right, targetColumn)
    case _ => false
  }

  private def toAccessPlan(filters: Array[Filter],
                           targetColumn: String,
                           targetField: Option[Primitive],
                           organization: VsamOrganization,
                           copybook: Copybook): Option[VsamAccessPlan] = {
    if (filters.isEmpty) {
      None
    } else {
      val flattened = filters.flatMap(flattenAnd)
      val equalityValues = flattened.collect { case EqualTo(attribute, value) if attribute.equalsIgnoreCase(targetColumn) => value }
      val inValues = flattened.collect { case In(attribute, values) if attribute.equalsIgnoreCase(targetColumn) => values }.flatten
      val lowerBounds = flattened.flatMap(extractLowerBound(_, targetColumn))
      val upperBounds = flattened.flatMap(extractUpperBound(_, targetColumn))

      if (equalityValues.nonEmpty || inValues.nonEmpty) {
        val values = (equalityValues ++ inValues).distinct
        Some(toPointPlan(values, targetField, organization, copybook))
      } else if (lowerBounds.nonEmpty || upperBounds.nonEmpty) {
        Some(toRangePlan(lowerBounds.sortBy(_._2).lastOption.map(_._1),
          upperBounds.sortBy(_._2).headOption.map(_._1),
          targetField,
          organization,
          copybook))
      } else {
        None
      }
    }
  }

  private def toPointPlan(values: Seq[Any], targetField: Option[Primitive], organization: VsamOrganization, copybook: Copybook): VsamAccessPlan = {
    organization match {
      case VsamOrganization.Ksds => VsamAccessPlan.KeyEquals(values.map(value => VsamKeyEncoding.encodeKey(copybook, targetField.get, value)))
      case _ => VsamAccessPlan.Positions(values.map(VsamKeyEncoding.toLong))
    }
  }

  private def toRangePlan(lowerBound: Option[Any],
                          upperBound: Option[Any],
                          targetField: Option[Primitive],
                          organization: VsamOrganization,
                          copybook: Copybook): VsamAccessPlan = {
    organization match {
      case VsamOrganization.Ksds =>
        VsamAccessPlan.KeyRange(lowerBound.map(value => VsamKeyEncoding.encodeKey(copybook, targetField.get, value)),
          upperBound.map(value => VsamKeyEncoding.encodeKey(copybook, targetField.get, value)))
      case _ =>
        VsamAccessPlan.PositionRange(lowerBound.map(VsamKeyEncoding.toLong), upperBound.map(VsamKeyEncoding.toLong))
    }
  }

  private def flattenAnd(filter: Filter): Seq[Filter] = filter match {
    case And(left, right) => flattenAnd(left) ++ flattenAnd(right)
    case other => Seq(other)
  }

  private def extractLowerBound(filter: Filter, targetColumn: String): Option[(Any, Boolean)] = filter match {
    case GreaterThan(attribute, value) if attribute.equalsIgnoreCase(targetColumn) => Some(value -> false)
    case GreaterThanOrEqual(attribute, value) if attribute.equalsIgnoreCase(targetColumn) => Some(value -> true)
    case _ => None
  }

  private def extractUpperBound(filter: Filter, targetColumn: String): Option[(Any, Boolean)] = filter match {
    case LessThan(attribute, value) if attribute.equalsIgnoreCase(targetColumn) => Some(value -> false)
    case LessThanOrEqual(attribute, value) if attribute.equalsIgnoreCase(targetColumn) => Some(value -> true)
    case _ => None
  }
}
