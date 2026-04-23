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

import com.ibm.jzos.ZFile
import com.ibm.jzos.ZFileConstants
import za.co.absa.cobrix.cobol.reader.parameters.VsamOrganization
import za.co.absa.cobrix.cobol.reader.parameters.VsamOrganization.{Esds, Ksds, Rrds}
import za.co.absa.cobrix.spark.cobol.source.vsam.VsamAccessPlan._

import scala.collection.mutable.ArrayBuffer

class JZosVsamSource(recordSize: Int) extends VsamSource {
  override def scan(dataset: VsamDataset, organization: VsamOrganization, accessPlan: VsamAccessPlan): Iterator[VsamRecord] = {
    val file = new ZFile(dataset.datasetName, "rb,type=record")
    val bufferSize = math.max(recordSize, file.getLrecl())
    val buffer = new Array[Byte](bufferSize)

    try {
      val rows: Vector[VsamRecord] = accessPlan match {
        case SequentialScan =>
          readSequential(file, buffer, organization).toVector
        case KeyEquals(keys) =>
          keys.iterator.flatMap(key => locateAndRead(file, buffer, organization, key = Some(key), position = None)).toVector
        case KeyRange(lowerBound, upperBound) =>
          readKeyRange(file, buffer, lowerBound, upperBound).toVector
        case Positions(values) =>
          values.iterator.flatMap(position => locateAndRead(file, buffer, organization, key = None, position = Some(position))).toVector
        case PositionRange(lowerBound, upperBound) =>
          readPositionRange(file, buffer, organization, lowerBound, upperBound).toVector
      }

      rows.iterator
    } finally {
      file.close()
    }
  }

  private def readSequential(file: ZFile, buffer: Array[Byte], organization: VsamOrganization): Iterator[VsamRecord] = {
    val rows = ArrayBuffer.empty[VsamRecord]
    var rrn = 1L
    var bytesRead = file.read(buffer)

    while (bytesRead >= 0) {
      rows += createRecord(buffer, bytesRead, organization, None, rrn)
      rrn += 1
      bytesRead = file.read(buffer)
    }

    rows.iterator
  }

  private def readKeyRange(file: ZFile, buffer: Array[Byte], lowerBound: Option[Array[Byte]], upperBound: Option[Array[Byte]]): Iterator[VsamRecord] = {
    val rows = ArrayBuffer.empty[VsamRecord]
    val positioned = lowerBound match {
      case Some(key) => file.locate(key, ZFileConstants.LOCATE_KEY_GE)
      case None => true
    }

    if (positioned) {
      var bytesRead = file.read(buffer)
      while (bytesRead >= 0 && upperBound.forall(upper => lexicographicCompare(buffer, bytesRead, upper) <= 0)) {
        rows += VsamRecord(buffer.take(bytesRead), None, None)
        bytesRead = file.read(buffer)
      }
    }

    rows.iterator
  }

  private def readPositionRange(file: ZFile,
                                buffer: Array[Byte],
                                organization: VsamOrganization,
                                lowerBound: Option[Long],
                                upperBound: Option[Long]): Iterator[VsamRecord] = {
    val rows = ArrayBuffer.empty[VsamRecord]

    lowerBound match {
      case Some(position) if file.locate(position, locateEq(organization)) =>
        var current = position
        var bytesRead = file.read(buffer)
        while (bytesRead >= 0 && upperBound.forall(current <= _)) {
          rows += createRecord(buffer, bytesRead, organization, Some(current), current)
          current += 1
          bytesRead = file.read(buffer)
        }
      case Some(_) =>
      case None =>
        rows ++= readSequential(file, buffer, organization)
    }

    rows.iterator
  }

  private def locateAndRead(file: ZFile,
                            buffer: Array[Byte],
                            organization: VsamOrganization,
                            key: Option[Array[Byte]],
                            position: Option[Long]): Option[VsamRecord] = {
    val found = key match {
      case Some(keyBytes) => file.locate(keyBytes, ZFileConstants.LOCATE_KEY_EQ)
      case None => file.locate(position.get, locateEq(organization))
    }

    if (found) {
      val bytesRead = file.read(buffer)
      if (bytesRead >= 0) {
        Some(createRecord(buffer, bytesRead, organization, position, position.getOrElse(0L)))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def createRecord(buffer: Array[Byte],
                           bytesRead: Int,
                           organization: VsamOrganization,
                           position: Option[Long],
                           rrn: Long): VsamRecord = {
    organization match {
      case Esds => VsamRecord(buffer.take(bytesRead), position, None)
      case Rrds => VsamRecord(buffer.take(bytesRead), None, Some(rrn))
      case Ksds => VsamRecord(buffer.take(bytesRead), None, None)
    }
  }

  private def locateEq(organization: VsamOrganization): Int = organization match {
    case Esds => ZFileConstants.LOCATE_RBA_EQ
    case Rrds => ZFileConstants.LOCATE_RBA_EQ
    case Ksds => ZFileConstants.LOCATE_KEY_EQ
  }

  private def lexicographicCompare(buffer: Array[Byte], length: Int, upperBound: Array[Byte]): Int = {
    val bytes = buffer.take(math.min(length, upperBound.length))
    bytes.zip(upperBound).collectFirst {
      case (left, right) if left != right => java.lang.Byte.toUnsignedInt(left) - java.lang.Byte.toUnsignedInt(right)
    }.getOrElse(length - upperBound.length)
  }
}
