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

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{EqualTo, StringStartsWith}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import za.co.absa.cobrix.cobol.reader.parameters.ReaderParameters
import za.co.absa.cobrix.cobol.reader.parameters.VsamOrganization.Esds
import za.co.absa.cobrix.cobol.reader.parameters.VsamParameters
import za.co.absa.cobrix.spark.cobol.parameters.MetadataFields.VSAM_RBA
import za.co.absa.cobrix.spark.cobol.reader.FixedLenReader
import za.co.absa.cobrix.spark.cobol.schema.CobolSchema
import za.co.absa.cobrix.spark.cobol.source.CobolRelation
import za.co.absa.cobrix.spark.cobol.source.base.SparkCobolTestBase
import za.co.absa.cobrix.spark.cobol.source.base.impl.DummyCobolSchema
import za.co.absa.cobrix.spark.cobol.source.parameters.LocalityParameters
import za.co.absa.cobrix.spark.cobol.source.vsam.VsamAccessPlan.Positions

import java.util.concurrent.atomic.AtomicReference

class VsamRelationSpec extends SparkCobolTestBase {
  import VsamRelationSpec._

  private val baseSchema = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("name", StringType, nullable = false)
  ))

  private val localityParams = new LocalityParameters(improveLocality = false, optimizeAllocation = false)

  "CobolRelation" should "scan VSAM records and project metadata columns" in {
      val source = new CapturingVsamSource(Seq(
        VsamRecord("01ALPHA".getBytes("UTF-8"), Some(100L), None),
        VsamRecord("02BRAVO".getBytes("UTF-8"), Some(200L), None)
      ))

      val relation = new CobolRelation(
        sourceDirs = Seq("vsam://HLQ.APP.CUSTOMER"),
        filesList = Array.empty,
        cobolReader = new StubFixedLenReader(baseSchema),
        localityParams = localityParams,
        debugIgnoreFileSize = false,
        vsamParams = Some(VsamParameters(Esds, None)),
        vsamSourceProvider = new FixedSourceProvider(source)
      )(sqlContext)

      val rows = relation.buildScan(Array("id", VSAM_RBA), Array(EqualTo(VSAM_RBA, 100L))).collect()

      assert(rows.length == 1)
      assert(rows.head.getString(0) == "01")
      assert(rows.head.getLong(1) == 100L)
      assert(source.lastAccessPlan.contains(Positions(Seq(100L))))
  }

  it should "apply residual filters after VSAM pushdown" in {
      val source = new CapturingVsamSource(Seq(
        VsamRecord("01ALPHA".getBytes("UTF-8"), Some(100L), None),
        VsamRecord("01BRAVO".getBytes("UTF-8"), Some(100L), None)
      ))

      val relation = new CobolRelation(
        sourceDirs = Seq("vsam://HLQ.APP.CUSTOMER"),
        filesList = Array.empty,
        cobolReader = new StubFixedLenReader(baseSchema),
        localityParams = localityParams,
        debugIgnoreFileSize = false,
        vsamParams = Some(VsamParameters(Esds, None)),
        vsamSourceProvider = new FixedSourceProvider(source)
      )(sqlContext)

      val rows = relation.buildScan(Array("id", "name"), Array(EqualTo(VSAM_RBA, 100L), StringStartsWith("name", "AL"))).collect()

      assert(rows.length == 1)
      assert(rows.head.getString(0) == "01")
      assert(rows.head.getString(1) == "ALPHA")
      assert(source.lastAccessPlan.contains(Positions(Seq(100L))))
  }
}

object VsamRelationSpec {
  private val lastAccessPlanRef = new AtomicReference[Option[VsamAccessPlan]](None)

  private class StubFixedLenReader(schema: StructType) extends FixedLenReader {
    override def getReaderProperties: ReaderParameters = ReaderParameters()
    override def getCobolSchema: CobolSchema = new DummyCobolSchema(schema)
    override def getSparkSchema: StructType = schema
    override def getRecordSize: Int = 7
    override def getRowIterator(binaryData: Array[Byte]): Iterator[Row] = {
      val raw = new String(binaryData, "UTF-8")
      Iterator(Row(raw.substring(0, 2), raw.substring(2)))
    }
  }

  private class CapturingVsamSource(records: Seq[VsamRecord]) extends VsamSource {
    lastAccessPlanRef.set(None)

    def lastAccessPlan: Option[VsamAccessPlan] = lastAccessPlanRef.get()

    override def scan(dataset: VsamDataset,
                      organization: za.co.absa.cobrix.cobol.reader.parameters.VsamOrganization,
                      accessPlan: VsamAccessPlan): Iterator[VsamRecord] = {
      lastAccessPlanRef.set(Some(accessPlan))
      accessPlan match {
        case Positions(values) => records.iterator.filter(_.rba.exists(values.contains))
        case _ => records.iterator
      }
    }
  }

  private class FixedSourceProvider(source: VsamSource) extends (Int => VsamSource) with Serializable {
    override def apply(recordSize: Int): VsamSource = source
  }
}
