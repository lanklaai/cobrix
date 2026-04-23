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

package za.co.absa.cobrix.cobol.reader.parameters

sealed trait VsamOrganization {
  def entryName: String
}

object VsamOrganization {
  case object Ksds extends VsamOrganization {
    override val entryName: String = "ksds"
  }

  case object Esds extends VsamOrganization {
    override val entryName: String = "esds"
  }

  case object Rrds extends VsamOrganization {
    override val entryName: String = "rrds"
  }

  private val byName = Seq(Ksds, Esds, Rrds).map(org => org.entryName -> org).toMap

  def fromString(value: String): Option[VsamOrganization] = byName.get(value.trim.toLowerCase)
}

case class VsamParameters(organization: VsamOrganization, keyColumn: Option[String])
