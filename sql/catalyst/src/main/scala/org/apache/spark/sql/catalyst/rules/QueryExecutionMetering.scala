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

package org.apache.spark.sql.catalyst.rules

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.AtomicLongMap

import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_SECOND

case class QueryExecutionMetering() {
  private val timeMap = AtomicLongMap.create[String]()
  private val numRunsMap = AtomicLongMap.create[String]()
  private val numEffectiveRunsMap = AtomicLongMap.create[String]()
  private val timeEffectiveRunsMap = AtomicLongMap.create[String]()

  private val perPlanTimeMap = AtomicLongMap.create[String]()
  private val perPlanNumRunsMap = AtomicLongMap.create[String]()
  private val perPlanNumEffectiveRunsMap = AtomicLongMap.create[String]()
  private val perPlanTimeEffectiveRunsMap = AtomicLongMap.create[String]()

  @inline
  private def groupIdPrefix(groupId: String) = s"[$groupId]"

  /** Resets statistics about time spent running specific rules */
  def resetMetrics(): Unit = {
    timeMap.clear()
    numRunsMap.clear()
    numEffectiveRunsMap.clear()
    timeEffectiveRunsMap.clear()

    perPlanTimeMap.clear()
    perPlanNumRunsMap.clear()
    perPlanNumEffectiveRunsMap.clear()
    perPlanTimeEffectiveRunsMap.clear()
  }

  /** Resets statistics about time spent running specific rules */
  def resetMetricsByGroupId(groupId: String): Unit = {

    def _clear_(map: AtomicLongMap[String]): Unit = {
      map.asMap().asScala.keys
        .filter(_.startsWith(groupIdPrefix(groupId))).foreach(key => perPlanTimeMap.remove(key))
    }

    _clear_(perPlanTimeMap)
    _clear_(perPlanNumRunsMap)
    _clear_(perPlanNumEffectiveRunsMap)
    _clear_(perPlanTimeEffectiveRunsMap)
  }

  def getMetrics(): QueryExecutionMetrics = {
    QueryExecutionMetrics(totalTime, totalNumRuns, totalNumEffectiveRuns, totalEffectiveTime)
  }

  def totalTime: Long = {
    timeMap.sum()
  }

  def totalNumRuns: Long = {
    numRunsMap.sum()
  }

  def totalNumEffectiveRuns: Long = {
    numEffectiveRunsMap.sum()
  }

  def totalEffectiveTime: Long = {
    timeEffectiveRunsMap.sum()
  }

  def incExecutionTimeBy(ruleName: String, delta: Long, groupId: String = ""): Unit = {
    timeMap.addAndGet(ruleName, delta)
    if (groupId.nonEmpty) {
      perPlanTimeMap.addAndGet(s"${groupIdPrefix(groupId)}$ruleName", delta)
    }
  }

  def incTimeEffectiveExecutionBy(ruleName: String, delta: Long, groupId: String = ""): Unit = {
    timeEffectiveRunsMap.addAndGet(ruleName, delta)
    if (groupId.nonEmpty) {
      perPlanTimeEffectiveRunsMap.addAndGet(s"${groupIdPrefix(groupId)}$ruleName", delta)
    }
  }

  def incNumEffectiveExecution(ruleName: String, groupId: String = ""): Unit = {
    numEffectiveRunsMap.incrementAndGet(ruleName)
    if (groupId.nonEmpty) {
      perPlanNumEffectiveRunsMap.incrementAndGet(s"${groupIdPrefix(groupId)}$ruleName")
    }
  }

  def incNumExecution(ruleName: String, groupId: String = ""): Unit = {
    numRunsMap.incrementAndGet(ruleName)
    if (groupId.nonEmpty) {
      perPlanNumRunsMap.incrementAndGet(s"${groupIdPrefix(groupId)}$ruleName")
    }
  }

  private def dump(
                    maxLengthRuleNames: Int,
                    totalNumRuns: Long,
                    totalTime: Long,
                    ruleMetrics: String, prefix: String = ""): String = {
    val colRuleName = "Rule".padTo(maxLengthRuleNames, " ").mkString
    val colRunTime = "Effective Time / Total Time".padTo(len = 47, " ").mkString
    val colNumRuns = "Effective Runs / Total Runs".padTo(len = 47, " ").mkString

    val header =
      if (prefix.isEmpty) "=== Metrics of Analyzer/Optimizer Rules ==="
      else s"=== $prefix Metrics of Analyzer/Optimizer Rules ==="

    s"""
       $header
       Total number of runs: $totalNumRuns
       Total time: ${totalTime / 1000000000D} seconds

       $colRuleName $colRunTime $colNumRuns
       $ruleMetrics
       """
  }

  def dumpPerPlanTimeSpent(groupId: String): String = {
    val prefix = groupIdPrefix(groupId)
    val map = perPlanTimeMap.asMap().asScala.filterKeys(_.startsWith(prefix))
    if (map.nonEmpty) {
      val maxLengthRuleNames = map.keys.map(_.length).max - prefix.length

      val ruleMetrics = map.toSeq.sortBy(_._2).reverseMap { case (name, time) =>
        val timeEffectiveRun = perPlanTimeEffectiveRunsMap.get(name)
        val numRuns = perPlanNumRunsMap.get(name)
        val numEffectiveRun = perPlanNumEffectiveRunsMap.get(name)

        val ruleName = name.substring(prefix.length).padTo(maxLengthRuleNames, " ").mkString
        val runtimeValue = s"$timeEffectiveRun / $time".padTo(len = 47, " ").mkString
        val numRunValue = s"$numEffectiveRun / $numRuns".padTo(len = 47, " ").mkString
        s"$ruleName $runtimeValue $numRunValue"
      }.mkString("\n", "\n", "")

      val _totalNumRuns: Long = perPlanNumRunsMap
        .asMap()
        .asScala
        .filterKeys(_.startsWith(prefix))
        .map(_._2.longValue())
        .sum
      val _totalTime: Long = perPlanTimeMap
        .asMap()
        .asScala
        .filterKeys(_.startsWith(prefix))
        .map(_._2.longValue())
        .sum

      dump(maxLengthRuleNames, _totalNumRuns, _totalTime, ruleMetrics, prefix)
    } else "Empty Metrics of Analyzer/Optimizer Rules"
  }

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxLengthRuleNames = if (map.isEmpty) {
      0
    } else {
      map.keys.map(_.length).max
    }

    val colRuleName = "Rule".padTo(maxLengthRuleNames, " ").mkString
    val colRunTime = "Effective Time / Total Time".padTo(len = 47, " ").mkString
    val colNumRuns = "Effective Runs / Total Runs".padTo(len = 47, " ").mkString

    val ruleMetrics = map.toSeq.sortBy(_._2).reverseMap { case (name, time) =>
      val timeEffectiveRun = timeEffectiveRunsMap.get(name)
      val numRuns = numRunsMap.get(name)
      val numEffectiveRun = numEffectiveRunsMap.get(name)

      val ruleName = name.padTo(maxLengthRuleNames, " ").mkString
      val runtimeValue = s"$timeEffectiveRun / $time".padTo(len = 47, " ").mkString
      val numRunValue = s"$numEffectiveRun / $numRuns".padTo(len = 47, " ").mkString
      s"$ruleName $runtimeValue $numRunValue"
    }.mkString("\n", "\n", "")

    dump(maxLengthRuleNames, totalNumRuns, totalTime, ruleMetrics)
  }
}

case class QueryExecutionMetrics(
    time: Long,
    numRuns: Long,
    numEffectiveRuns: Long,
    timeEffective: Long) {

  def -(metrics: QueryExecutionMetrics): QueryExecutionMetrics = {
    QueryExecutionMetrics(
      this.time - metrics.time,
      this.numRuns - metrics.numRuns,
      this.numEffectiveRuns - metrics.numEffectiveRuns,
      this.timeEffective - metrics.timeEffective)
  }
}
