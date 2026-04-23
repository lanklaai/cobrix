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

package za.co.absa.cobrix.spark.cobol.source.parameters

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.cobrix.cobol.parser.policies.VariableSizeOccursPolicy
import za.co.absa.cobrix.cobol.parser.recordformats.RecordFormat.{FixedLength, VariableBlock}
import za.co.absa.cobrix.cobol.reader.parameters.{MultisegmentParameters, ReaderParameters}
import za.co.absa.cobrix.cobol.reader.parameters.CobolParametersParser._
import org.apache.hadoop.conf.Configuration

class CobolParametersValidatorSuite extends AnyWordSpec {
  "validateOrThrow" should {
    "reject VSAM path without organization" in {
      val ex = intercept[IllegalArgumentException] {
        CobolParametersValidator.validateOrThrow(Map(
          PARAM_SOURCE_PATH -> "vsam://HLQ.APP.CUSTOMER",
          PARAM_COPYBOOK_CONTENTS -> "01 REC. 05 CUSTOMER-ID PIC X(10)."
        ), new Configuration(false))
      }

      assert(ex.getMessage.contains(PARAM_VSAM_ORGANIZATION))
    }
  }

  "checkSanity" should {
    "reject KSDS without key column" in {
      val ex = intercept[IllegalArgumentException] {
        val params = za.co.absa.cobrix.cobol.reader.parameters.CobolParametersParser.parse(
          new za.co.absa.cobrix.cobol.reader.parameters.Parameters(Map(
            PARAM_SOURCE_PATH -> "vsam://HLQ.APP.CUSTOMER",
            PARAM_COPYBOOK_CONTENTS -> "01 REC. 05 CUSTOMER-ID PIC X(10).",
            PARAM_RECORD_FORMAT -> "F",
            PARAM_VSAM_ORGANIZATION -> "ksds"
          ))
        )
        CobolParametersValidator.checkSanity(params)
      }

      assert(ex.getMessage.contains(PARAM_VSAM_KEY_COLUMN))
    }

    "reject key column for ESDS and RRDS" in {
      Seq("esds", "rrds").foreach { organization =>
        val ex = intercept[IllegalArgumentException] {
          val params = za.co.absa.cobrix.cobol.reader.parameters.CobolParametersParser.parse(
            new za.co.absa.cobrix.cobol.reader.parameters.Parameters(Map(
              PARAM_SOURCE_PATH -> "vsam://HLQ.APP.CUSTOMER",
              PARAM_COPYBOOK_CONTENTS -> "01 REC. 05 CUSTOMER-ID PIC X(10).",
              PARAM_RECORD_FORMAT -> "F",
              PARAM_VSAM_ORGANIZATION -> organization,
              PARAM_VSAM_KEY_COLUMN -> "CUSTOMER-ID"
            ))
          )
          CobolParametersValidator.checkSanity(params)
        }

        assert(ex.getMessage.contains(PARAM_VSAM_KEY_COLUMN))
      }
    }

    "reject unsupported VSAM organization" in {
      val ex = intercept[IllegalArgumentException] {
        za.co.absa.cobrix.cobol.reader.parameters.CobolParametersParser.parse(
          new za.co.absa.cobrix.cobol.reader.parameters.Parameters(Map(
            PARAM_SOURCE_PATH -> "vsam://HLQ.APP.CUSTOMER",
            PARAM_COPYBOOK_CONTENTS -> "01 REC. 05 CUSTOMER-ID PIC X(10).",
            PARAM_RECORD_FORMAT -> "F",
            PARAM_VSAM_ORGANIZATION -> "lds"
          ))
        )
      }

      assert(ex.getMessage.contains("Unsupported VSAM organization"))
    }
  }

  "validateParametersForWriting" should {
    "detect validation issues" in {
      val readParams = ReaderParameters(
        recordFormat = VariableBlock,
        occursMappings = Map("A" -> Map("B" -> 1)),
        startOffset = 1,
        fileEndOffset = 2,
        multisegment = Some(MultisegmentParameters("SEG", None, Seq.empty, "", Map.empty, Map.empty))
      )

      val ex = intercept[IllegalArgumentException] {
        CobolParametersValidator.validateParametersForWriting(readParams)
      }

      assert(ex.getMessage.contains("Writer validation issues: Only 'F' and 'V' values for 'record_format' are supported for writing, provided value: 'VB';"))
      assert(ex.getMessage.contains("OCCURS mapping option ('occurs_mappings') is not supported for writing"))
      assert(ex.getMessage.contains("'record_start_offset' and 'record_end_offset' are not supported for writing"))
      assert(ex.getMessage.contains("'file_start_offset' and 'file_end_offset' are not supported for writing"))
      assert(ex.getMessage.contains("Multi-segment options ('segment_field', 'segment_filter', etc) are not supported for writing"))
    }

    "do not throw exceptions if the configuration is okay" in {
      val readParams = ReaderParameters(
        recordFormat = FixedLength,
        variableSizeOccurs = VariableSizeOccursPolicy.ShiftRecord
      )

      CobolParametersValidator.validateParametersForWriting(readParams)
    }
  }

}
