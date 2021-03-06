/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.types.logical.{DecimalType, DoubleType, LogicalType}

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, RelDataTypeSystemImpl}
import org.apache.calcite.sql.`type`.SqlTypeName

/**
  * Custom type system for Flink.
  * Flink的自定义类型系统。
  */
class FlinkTypeSystem extends RelDataTypeSystemImpl {

  // set the maximum precision of a NUMERIC or DECIMAL type to DecimalType.MAX_PRECISION.
  override def getMaxNumericPrecision: Int = DecimalType.MAX_PRECISION

  // the max scale can't be greater than precision
  override def getMaxNumericScale: Int = DecimalType.MAX_PRECISION

  override def getDefaultPrecision(typeName: SqlTypeName): Int = typeName match {

    // Calcite will limit the length of the VARCHAR field to 65536
    case SqlTypeName.VARCHAR | SqlTypeName.VARBINARY =>
      Int.MaxValue

    // we currently support only timestamps with milliseconds precision
    case SqlTypeName.TIMESTAMP | SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
      3

    case _ =>
      super.getDefaultPrecision(typeName)
  }

  override def getMaxPrecision(typeName: SqlTypeName): Int = typeName match {
    case SqlTypeName.VARCHAR | SqlTypeName.CHAR | SqlTypeName.VARBINARY | SqlTypeName.BINARY =>
      Int.MaxValue

    case _ =>
      super.getMaxPrecision(typeName)
  }

  // when union a number of CHAR types of different lengths, we should cast to a VARCHAR
  // this fixes the problem of CASE WHEN with different length string literals but get wrong
  // result with additional space suffix
  // 当联合一些不同长度的CHAR类型时，我们应该强制转换为VARCHAR，这解决了CASE WHEN的问题，
  // 使用不同长度的字符串文字，但是得到错误的结果并带有额外的空格后缀
  override def shouldConvertRaggedUnionTypesToVarying(): Boolean = true

  // 产生平均聚合类型
  override def deriveAvgAggType(
      typeFactory: RelDataTypeFactory, argType: RelDataType): RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toLogicalType(argType)
    val avgType = FlinkTypeSystem.deriveAvgAggType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createFieldTypeFromLogicalType(
      avgType.copy(argType.isNullable))
  }

  // 产生和类型
  override def deriveSumType(
      typeFactory: RelDataTypeFactory, argType: RelDataType): RelDataType = {
    val argTypeInfo = FlinkTypeFactory.toLogicalType(argType)
    val sumType = FlinkTypeSystem.deriveSumType(argTypeInfo)
    typeFactory.asInstanceOf[FlinkTypeFactory].createFieldTypeFromLogicalType(
      sumType.copy(argType.isNullable))
  }
}

object FlinkTypeSystem {

  def deriveAvgAggType(argType: LogicalType): LogicalType = argType match {
    case dt: DecimalType =>
      val result = inferAggAvgType(dt.getScale)
      new DecimalType(result.getPrecision, result.getScale)
    case nt if TypeCheckUtils.isNumeric(nt) => nt
    case _ =>
      throw new RuntimeException("Unsupported argType for AVG(): " + argType)
  }

  def deriveSumType(argType: LogicalType): LogicalType = argType match {
    case dt: DecimalType =>
      val result = inferAggSumType(dt.getScale())
      new DecimalType(result.getPrecision(), result.getScale())
    case nt if TypeCheckUtils.isNumeric(nt) =>
      argType
    case _ =>
      throw new RuntimeException("Unsupported argType for SUM(): " + argType)
  }

  /**
    * https://docs.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql.
    */
  def inferDivisionType(
      precision1: Int, scale1: Int, precision2: Int, scale2: Int): DecimalType = {
    // note: magic numbers are used directly here, because it's not really a general algorithm.
    var scale = Math.max(6, scale1 + precision2 + 1)
    var precision = precision1 - scale1 + scale2 + scale
    if (precision > 38) {
      scale = Math.max(6, 38 - (precision - scale))
      precision = 38
    }
    new DecimalType(precision, scale)
  }

  def inferIntDivType(precision1: Int, scale1: Int, scale2: Int): DecimalType = {
    val p = Math.min(38, precision1 - scale1 + scale2)
    new DecimalType(p, 0)
  }

  /**
    * https://docs.microsoft.com/en-us/sql/t-sql/functions/sum-transact-sql.
    */
  def inferAggSumType(scale: Int) = new DecimalType(38, scale)

  /**
    * https://docs.microsoft.com/en-us/sql/t-sql/functions/avg-transact-sql
    * however, we count by LONG, therefore divide by Decimal(20,0),
    * but the end result is actually the same, which is Decimal(38, max(6,s)).
    */
  def inferAggAvgType(scale: Int): DecimalType = inferDivisionType(38, scale, 20, 0)

  /**
    * return type of Round( DECIMAL(p,s), r).
    */
  def inferRoundType(precision: Int, scale: Int, r: Int): DecimalType = {
    if (r >= scale) new DecimalType(precision, scale)
    else if (r < 0) new DecimalType(Math.min(38, 1 + precision - scale), 0)
    else { // 0 <= r < s
      new DecimalType(1 + precision - scale + r, r)
    }
    // NOTE: rounding may increase the digits by 1, therefore we need +1 on precisions.
  }

  val DECIMAL_SYSTEM_DEFAULT = new DecimalType(DecimalType.MAX_PRECISION, 18)
}
