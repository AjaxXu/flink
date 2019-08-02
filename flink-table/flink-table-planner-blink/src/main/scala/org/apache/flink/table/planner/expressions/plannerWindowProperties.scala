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

package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api.TableException
import org.apache.flink.table.types.logical.{BigIntType, LogicalType, TimestampKind, TimestampType}

trait PlannerWindowProperty {
  def resultType: LogicalType
}

abstract class AbstractPlannerWindowProperty(
    reference: PlannerWindowReference) extends PlannerWindowProperty {
  override def toString = s"WindowProperty($reference)"
}

/**
  * Indicate timeField type.
  * 指示timeField类型。
  */
case class PlannerWindowReference(name: String, tpe: Option[LogicalType] = None) {
  override def toString: String = s"'$name"
}

case class PlannerWindowStart(
    reference: PlannerWindowReference) extends AbstractPlannerWindowProperty(reference) {

  override def resultType: TimestampType = new TimestampType(3)

  override def toString: String = s"start($reference)"
}

case class PlannerWindowEnd(
    reference: PlannerWindowReference) extends AbstractPlannerWindowProperty(reference) {

  override def resultType: TimestampType = new TimestampType(3)

  override def toString: String = s"end($reference)"
}

case class PlannerRowtimeAttribute(
    reference: PlannerWindowReference) extends AbstractPlannerWindowProperty(reference) {

  override def resultType: LogicalType = {
    reference match {
      case PlannerWindowReference(_, Some(tpe))
        if tpe.isInstanceOf[TimestampType] &&
            tpe.asInstanceOf[TimestampType].getKind == TimestampKind.ROWTIME =>
        // rowtime window
        new TimestampType(true, TimestampKind.ROWTIME, 3)
      case PlannerWindowReference(_, Some(tpe))
        if tpe.isInstanceOf[BigIntType] || tpe.isInstanceOf[TimestampType] =>
        // batch time window
        new TimestampType(3)
      case _ =>
        throw new TableException("WindowReference of RowtimeAttribute has invalid type. " +
            "Please report this bug.")
    }
  }

  override def toString: String = s"rowtime($reference)"
}

case class PlannerProctimeAttribute(reference: PlannerWindowReference)
  extends AbstractPlannerWindowProperty(reference) {

  override def resultType: LogicalType =
    new TimestampType(true, TimestampKind.PROCTIME, 3)

  override def toString: String = s"proctime($reference)"
}
