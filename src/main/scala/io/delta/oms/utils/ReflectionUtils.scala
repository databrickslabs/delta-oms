/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.oms.utils

import scala.reflect.runtime.universe._

object ReflectionUtils {

  def getDataTypeByColumnName[T: TypeTag](colName: String): String = {
    accessClassMembers[T].find(m => colName.equals(m.name.toString))
      .get.typeSignature.resultType.toString
  }

  def accessClassMembers[T: TypeTag]: Seq[MethodSymbol] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList
}
