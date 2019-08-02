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

import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexLocalRef

/**
  * Special reference which represent a local filed, such as aggregate buffers or constants.
  * We are stored as class members, so the field can be referenced directly.
  * We should use an unique name to locate the field.
  * 表示本地字段的特殊引用，例如聚合缓冲区或常量。我们存储为类成员，因此可以直接引用该字段。
  * 我们应该使用唯一的名称来定位该字段。
  *
  * See [[org.apache.flink.table.planner.codegen.ExprCodeGenerator.visitLocalRef()]]
  */
case class RexAggLocalVariable(
    fieldTerm: String,
    nullTerm: String,
    dataType: RelDataType,
    internalType: LogicalType)
  extends RexLocalRef(0, dataType)

/**
  * Special reference which represent a distinct key input filed,
  * We use the name to locate the distinct key field.
  *
  * See [[org.apache.flink.table.planner.codegen.ExprCodeGenerator.visitLocalRef()]]
  */
case class RexDistinctKeyVariable(
    keyTerm: String,
    dataType: RelDataType,
    internalType: LogicalType)
  extends RexLocalRef(0, dataType)
