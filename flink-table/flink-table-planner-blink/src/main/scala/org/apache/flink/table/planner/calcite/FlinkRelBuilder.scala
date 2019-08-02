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

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.FunctionCatalog
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkRelFactories.{ExpandFactory, RankFactory, SinkFactory}
import org.apache.flink.table.planner.expressions.{PlannerWindowProperty, WindowProperty}
import org.apache.flink.table.planner.plan.QueryOperationConverter
import org.apache.flink.table.planner.plan.logical.LogicalWindow
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate
import org.apache.flink.table.runtime.operators.rank.{RankRange, RankType}
import org.apache.flink.table.sinks.TableSink

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeField}
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.calcite.tools.{RelBuilder, RelBuilderFactory}
import org.apache.calcite.util.{ImmutableBitSet, Util}

import java.lang.Iterable
import java.util
import java.util.List

import scala.collection.JavaConversions._

/**
  * Flink specific [[RelBuilder]] that changes the default type factory to a [[FlinkTypeFactory]].
  * Flink特定[[RelBuilder]]将默认类型工厂更改为[[FlinkTypeFactory]]。
  */
class FlinkRelBuilder(
    context: Context,
    relOptCluster: RelOptCluster,
    relOptSchema: RelOptSchema)
  extends RelBuilder(
    context,
    relOptCluster,
    relOptSchema) {

  require(context != null)

  private val toRelNodeConverter = {
    val functionCatalog = context.asInstanceOf[FlinkContext].getFunctionCatalog
    new QueryOperationConverter(this, functionCatalog)
  }

  private val expandFactory: ExpandFactory = {
    Util.first(context.unwrap(classOf[ExpandFactory]), FlinkRelFactories.DEFAULT_EXPAND_FACTORY)
  }

  private val rankFactory: RankFactory = {
    Util.first(context.unwrap(classOf[RankFactory]), FlinkRelFactories.DEFAULT_RANK_FACTORY)
  }

  private val sinkFactory: SinkFactory = {
    Util.first(context.unwrap(classOf[SinkFactory]), FlinkRelFactories.DEFAULT_SINK_FACTORY)
  }

  def getRelOptSchema: RelOptSchema = relOptSchema

  def getCluster: RelOptCluster = relOptCluster

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]

  def expand(
      outputRowType: RelDataType,
      projects: util.List[util.List[RexNode]],
      expandIdIndex: Int): RelBuilder = {
    val input = build()
    val expand = expandFactory.createExpand(input, outputRowType, projects, expandIdIndex)
    push(expand)
  }

  def sink(sink: TableSink[_], sinkName: String): RelBuilder = {
    val input = build()
    val sinkNode = sinkFactory.createSink(input, sink, sinkName)
    push(sinkNode)
  }

  def rank(
      partitionKey: ImmutableBitSet,
      orderKey: RelCollation,
      rankType: RankType,
      rankRange: RankRange,
      rankNumberType: RelDataTypeField,
      outputRankNumber: Boolean): RelBuilder = {
    val input = build()
    val rank = rankFactory.createRank(input, partitionKey, orderKey, rankType, rankRange,
      rankNumberType, outputRankNumber)
    push(rank)
  }

  def aggregate(
      window: LogicalWindow,
      groupKey: GroupKey,
      namedProperties: List[PlannerNamedWindowProperty],
      aggCalls: Iterable[AggCall]): RelBuilder = {
    // build logical aggregate
    val aggregate = super.aggregate(groupKey, aggCalls).build().asInstanceOf[LogicalAggregate]

    // build logical window aggregate from it
    push(LogicalWindowAggregate.create(window, namedProperties, aggregate))
    this
  }

  def queryOperation(queryOperation: QueryOperation): RelBuilder = {
    val relNode = queryOperation.accept(toRelNodeConverter)
    push(relNode)
    this
  }
}

object FlinkRelBuilder {

  /**
    * Information necessary to create a window aggregate.
    * 创建窗口聚合所需的信息。
    *
    * Similar to [[RelBuilder.AggCall]] or [[RelBuilder.GroupKey]].
    */
  case class PlannerNamedWindowProperty(name: String, property: PlannerWindowProperty)

  case class NamedWindowProperty(name: String, property: WindowProperty)

  def proto(context: Context): RelBuilderFactory = new RelBuilderFactory() {
    def create(cluster: RelOptCluster, schema: RelOptSchema): RelBuilder = {

      val clusterContext = cluster.getPlanner.getContext.asInstanceOf[FlinkContext]

      val mergedContext = new FlinkContext {

        override def getTableConfig: TableConfig = clusterContext.getTableConfig

        override def getFunctionCatalog: FunctionCatalog = clusterContext.getFunctionCatalog

        override def unwrap[C](clazz: Class[C]): C = context.unwrap(clazz)
      }
      new FlinkRelBuilder(mergedContext, cluster, schema)
    }
  }

  def of(cluster: RelOptCluster, relTable: RelOptTable): FlinkRelBuilder = {
    val clusterContext = cluster.getPlanner.getContext
    new FlinkRelBuilder(
      clusterContext,
      cluster,
      relTable.getRelOptSchema)
  }
}
