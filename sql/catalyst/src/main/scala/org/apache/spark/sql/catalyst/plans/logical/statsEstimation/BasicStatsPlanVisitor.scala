/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A [[LogicalPlanVisitor]] that computes the statistics for the cost-based optimizer.
 */
object BasicStatsPlanVisitor extends LogicalPlanVisitor[Statistics] with Logging {

  /** Falls back to the estimation computed by [[SizeInBytesOnlyStatsPlanVisitor]]. */
  private def fallback(p: LogicalPlan): Statistics = SizeInBytesOnlyStatsPlanVisitor.visit(p)

  override def default(p: LogicalPlan): Statistics = p match {
    case p: LeafNode =>
      val statistics = p.computeStats()
      logDebug(s"default - LeafNode (${p.nodeName}) : $statistics; " +
        s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
      statistics
    case _: LogicalPlan =>
      val stats = p.children.map(_.stats)
      val rowCount = if (stats.exists(_.rowCount.isEmpty)) {
        None
      } else {
        Some(stats.map(_.rowCount.get).filter(_ > 0L).product)
      }
      val statistics = Statistics(sizeInBytes = stats.map(_.sizeInBytes).filter(_ > 0L).product,
        rowCount = rowCount)
      logDebug(s"default - LogicalPlan  (${p.nodeName}) : $statistics; " +
        s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
      statistics
  }

  override def visitAggregate(p: Aggregate): Statistics = {
    val statistics = AggregateEstimation.estimate(p).getOrElse(fallback(p))
    logDebug(s"visitAggregate : $statistics; " +
      s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
    statistics
  }

  override def visitDistinct(p: Distinct): Statistics = {
    val child = p.child
    visitAggregate(Aggregate(child.output, child.output, child))
  }

  override def visitExcept(p: Except): Statistics = fallback(p)

  override def visitExpand(p: Expand): Statistics = fallback(p)

  override def visitFilter(p: Filter): Statistics = {
    val statistics = FilterEstimation(p).estimate.getOrElse(fallback(p))
    logDebug(s"visitFilter : $statistics; " +
      s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
    statistics
  }

  override def visitGenerate(p: Generate): Statistics = default(p)

  override def visitGlobalLimit(p: GlobalLimit): Statistics = fallback(p)

  override def visitOffset(p: Offset): Statistics = fallback(p)

  override def visitIntersect(p: Intersect): Statistics = {
    val leftStats = p.left.stats
    val rightStats = p.right.stats
    val leftSize = leftStats.sizeInBytes
    val rightSize = rightStats.sizeInBytes
    if (leftSize < rightSize) {
      Statistics(sizeInBytes = leftSize, rowCount = leftStats.rowCount)
    } else {
      Statistics(sizeInBytes = rightSize, rowCount = rightStats.rowCount)
    }
  }

  override def visitJoin(p: Join): Statistics = {
    val statistics = JoinEstimation(p).estimate.getOrElse(fallback(p))
    logDebug(s"visitJoin : $statistics; " +
      s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
    statistics
  }

  override def visitLocalLimit(p: LocalLimit): Statistics = fallback(p)

  override def visitPivot(p: Pivot): Statistics = default(p)

  override def visitProject(p: Project): Statistics = {
    val statistics = ProjectEstimation.estimate(p).getOrElse(fallback(p))
    logDebug(s"visitProject : $statistics; " +
      s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
    statistics
  }

  override def visitRepartition(p: Repartition): Statistics = fallback(p)

  override def visitRepartitionByExpr(p: RepartitionByExpression): Statistics = fallback(p)

  override def visitRebalancePartitions(p: RebalancePartitions): Statistics = fallback(p)

  override def visitSample(p: Sample): Statistics = fallback(p)

  override def visitScriptTransform(p: ScriptTransformation): Statistics = default(p)

  override def visitUnion(p: Union): Statistics = {
    val statistics = UnionEstimation.estimate(p).getOrElse(fallback(p))
    logDebug(s"visitUnion : $statistics; " +
      s"${p.schema.fields.map(_.name).mkString("[", ", ", "]")}")
    statistics
  }

  override def visitWindow(p: Window): Statistics = fallback(p)

  override def visitSort(p: Sort): Statistics = fallback(p)

  override def visitTail(p: Tail): Statistics = {
    fallback(p)
  }

  override def visitWithCTE(p: WithCTE): Statistics = fallback(p)
}
