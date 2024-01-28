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

import org.slf4j.MDC

import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A trait to add statistics propagation to [[LogicalPlan]].
 */
trait LogicalPlanStats { self: LogicalPlan =>

  def updateStatsCache(sizeInBytes: BigInt, rowCount: Option[BigInt],
                       metadata: Map[String, String]): Unit = {
    if (statsCache.isDefined) {
      statsCache = Some(statsCache.get.copy(sizeInBytes = sizeInBytes, rowCount = rowCount,
        metadata = metadata))
    } else {
      statsCache = Some(Statistics(sizeInBytes = sizeInBytes, rowCount = rowCount,
        metadata = metadata))
    }
  }

  def updateStatsCache(sizeInBytes: BigInt, rowCount: Option[BigInt]): Unit = {
    updateStatsCache(sizeInBytes, rowCount, Map.empty)
  }

  def updateStatsCache(stats: Statistics): Unit = {
    statsCache = Some(stats)
  }

  def updateStatsCache(rowCount: BigInt, metadata: Map[String, String] = Map.empty): Unit = {
    val outputRowSize = EstimationUtils.getSizePerRow(this.output)
    updateStatsCache(outputRowSize * rowCount, Some(rowCount), metadata)
  }

  /**
   * Returns the estimated statistics for the current logical plan node. Under the hood, this
   * method caches the return value, which is computed based on the configuration passed in the
   * first time. If the configuration changes, the cache can be invalidated by calling
   * [[invalidateStatsCache()]].
   */
  def stats: Statistics = statsCache.getOrElse {
    if (conf.cboEnabled) {
      statsCache = Option(LogicalPlanVisitor.cbo.visit(self))
    } else {
      statsCache = Option(LogicalPlanVisitor.nonCbo.visit(self))
    }
    val id = MDC.get("statsCalcId")
    val value = statsCache.get
    if (id != null && !value.metadata.contains("statsCalcId")) {
      statsCache = Some(value.copy(metadata = Map("statsCalcId" -> id)))
    }

    statsCache.get
  }

  /** A cache for the estimated statistics, such that it will only be computed once. */
  protected var statsCache: Option[Statistics] = None

  /** Invalidates the stats cache. See [[stats]] for more information. */
  final def invalidateStatsCache(): Unit = {
    statsCache = None
    children.foreach(_.invalidateStatsCache())
  }
}
