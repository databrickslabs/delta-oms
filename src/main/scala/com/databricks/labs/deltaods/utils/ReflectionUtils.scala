package com.databricks.labs.deltaods.utils

import scala.reflect.runtime.universe._
object ReflectionUtils {

  def accessClassMembers[T: TypeTag]: Seq[MethodSymbol] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toList

  def getDataTypeByColumnName[T: TypeTag](colName: String): String = {
    accessClassMembers[T].find(m => colName.equals(m.name.toString))
      .get.typeSignature.resultType.toString
  }
}
