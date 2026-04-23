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
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.cobrix.cobol.parser.CopybookParser
import za.co.absa.cobrix.cobol.reader.parameters.{VsamOrganization, VsamParameters}

class VsamFilterPushdownSpec extends AnyWordSpec {
  private val copybook = CopybookParser.parseTree(
    """       01  CUSTOMER-REC.
      |          05  CUSTOMER-ID        PIC X(10).
      |          05  CUSTOMER-NAME      PIC X(20).
      |""".stripMargin)

  "VsamFilterPushdown" should {
    "push down KSDS equality and keep unsupported filters residual" in {
      val plan = VsamFilterPushdown.plan(Array(
        EqualTo("CUSTOMER-ID", "0001"),
        EqualTo("CUSTOMER-NAME", "ALPHA")
      ), VsamParameters(VsamOrganization.Ksds, Some("CUSTOMER-ID")), copybook)

      assert(plan.accessPlan.isInstanceOf[VsamAccessPlan.KeyEquals])
      assert(plan.pushedFilters.length == 1)
      assert(plan.residualFilters.length == 1)
    }

    "push down ESDS position ranges" in {
      val plan = VsamFilterPushdown.plan(Array(
        GreaterThanOrEqual("_vsam_rba", 100L),
        LessThan("_vsam_rba", 500L)
      ), VsamParameters(VsamOrganization.Esds, None), copybook)

      val access = plan.accessPlan.asInstanceOf[VsamAccessPlan.PositionRange]
      assert(access.lowerBound.contains(100L))
      assert(access.upperBound.contains(500L))
      assert(plan.residualFilters.isEmpty)
    }

    "fall back to sequential scan for unsupported predicates" in {
      val plan = VsamFilterPushdown.plan(Array(
        Or(EqualTo("CUSTOMER-ID", "0001"), EqualTo("CUSTOMER-ID", "0002"))
      ), VsamParameters(VsamOrganization.Ksds, Some("CUSTOMER-ID")), copybook)

      assert(plan.accessPlan == VsamAccessPlan.SequentialScan)
      assert(plan.residualFilters.length == 1)
    }
  }
}
