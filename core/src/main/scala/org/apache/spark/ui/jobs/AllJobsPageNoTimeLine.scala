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

package org.apache.spark.ui.jobs

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.JobExecutionStatus
import org.apache.spark.scheduler._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.apache.spark.ui._
import org.apache.spark.util.Utils

import java.net.URLEncoder
import java.util.Date
import javax.servlet.http.HttpServletRequest
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.xml._

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPageNoTimeLine(parent: JobsTabNoTimeLine, store: AppStatusStore) extends WebUIPage("") {

  import ApiHelper._

  private val JOBS_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="succeeded-job-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Succeeded</text>
      <rect class="failed-job-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="running-job-legend"
        x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Running</text>
    </svg></div>.toString.filter(_ != '\n')

  private val EXECUTORS_LEGEND =
    <div class="legend-area"><svg width="150px" height="55px">
      <rect class="executor-added-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Added</text>
      <rect class="executor-removed-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Removed</text>
    </svg></div>.toString.filter(_ != '\n')

  private def makeJobEvent(jobs: Seq[v1.JobData]): Seq[String] = {
    jobs.filter { job =>
      job.status != JobExecutionStatus.UNKNOWN && job.submissionTime.isDefined
    }.map { job =>
      val jobId = job.jobId
      val status = job.status
      val (_, lastStageDescription) = lastStageNameAndDescription(store, job)
      val jobDescription = UIUtils.makeDescription(lastStageDescription, "", plainText = true).text

      val submissionTime = job.submissionTime.get.getTime()
      val completionTime = job.completionTime.map(_.getTime()).getOrElse(System.currentTimeMillis())
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a Javascript string literal.
      val escapedDesc = Utility.escape(jobDescription)
      val jsEscapedDesc = StringEscapeUtils.escapeEcmaScript(escapedDesc)
      val jobEventJsonAsStr =
        s"""
 {
   'className': 'job application-timeline-object ${classNameByStatus}',
   'group': 'jobs',
   'start': new Date(${submissionTime}),
   'end': new Date(${completionTime}),
   'content': '<div class="application-timeline-content"' +
      'data-html="true" data-placement="top" data-toggle="tooltip"' +
      'data-title="${jsEscapedDesc} (Job ${jobId})<br>' +
      'Status: ${status}<br>' +
      'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
      '${
          if (status != JobExecutionStatus.RUNNING) {
            s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
          } else {
            ""
          }
       }">' +
     '${jsEscapedDesc} (Job ${jobId})</div>'
 }
"""
      jobEventJsonAsStr
    }
  }

  private def makeExecutorEvent(executors: Seq[v1.ExecutorSummary]):
      Seq[String] = {
    val events = ListBuffer[String]()
    executors.foreach { e =>
      val addedEvent =
        s"""
{
  'className': 'executor added',
  'group': 'executors',
  'start': new Date(${e.addTime.getTime()}),
  'content': '<div class="executor-event-content"' +
    'data-toggle="tooltip" data-placement="bottom"' +
    'data-title="Executor ${e.id}<br>' +
    'Added at ${UIUtils.formatDate(e.addTime)}"' +
    'data-html="true">Executor ${e.id} added</div>'
}
"""
      events += addedEvent

      e.removeTime.foreach { removeTime =>
        val removedEvent =
          s"""
{
  'className': 'executor removed',
  'group': 'executors',
  'start': new Date(${removeTime.getTime()}),
  'content': '<div class="executor-event-content"' +
    'data-toggle="tooltip" data-placement="bottom"' +
    'data-title="Executor ${e.id}<br>' +
    'Removed at ${UIUtils.formatDate(removeTime)}' +
    '${
        e.removeReason.map { reason =>
          s"""<br>Reason: ${reason.replace("\n", " ")}"""
        }.getOrElse("")
     }"' +
    'data-html="true">Executor ${e.id} removed</div>'
}
"""
        events += removedEvent
      }
    }
    events.toSeq
  }

  private def jobsTable(
      request: HttpServletRequest,
      tableHeaderId: String,
      jobTag: String,
      jobs: Seq[v1.JobData],
      killEnabled: Boolean): Seq[Node] = {
    // stripXSS is called to remove suspicious characters used in XSS attacks
    val allParameters = request.getParameterMap.asScala.toMap.map { case (k, v) =>
      UIUtils.stripXSS(k) -> v.map(UIUtils.stripXSS).toSeq
    }
    val parameterOtherTable = allParameters.filterNot(_._1.startsWith(jobTag))
      .map(para => para._1 + "=" + para._2(0))

    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)
    val jobIdTitle = if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"

    // stripXSS is called first to remove suspicious characters used in XSS attacks
    val parameterJobPage = UIUtils.stripXSS(request.getParameter(jobTag + ".page"))
    val parameterJobSortColumn = UIUtils.stripXSS(request.getParameter(jobTag + ".sort"))
    val parameterJobSortDesc = UIUtils.stripXSS(request.getParameter(jobTag + ".desc"))
    val parameterJobPageSize = UIUtils.stripXSS(request.getParameter(jobTag + ".pageSize"))
    val parameterJobPrevPageSize = UIUtils.stripXSS(request.getParameter(jobTag + ".prevPageSize"))

    val jobPage = Option(parameterJobPage).map(_.toInt).getOrElse(1)
    val jobSortColumn = Option(parameterJobSortColumn).map { sortColumn =>
      UIUtils.decodeURLParameter(sortColumn)
    }.getOrElse(jobIdTitle)
    val jobSortDesc = Option(parameterJobSortDesc).map(_.toBoolean).getOrElse(
      // New jobs should be shown above old jobs by default.
      jobSortColumn == jobIdTitle
    )
    val jobPageSize = Option(parameterJobPageSize).map(_.toInt).getOrElse(100)
    val jobPrevPageSize = Option(parameterJobPrevPageSize).map(_.toInt).getOrElse(jobPageSize)

    val page: Int = {
      // If the user has changed to a larger page size, then go to page 1 in order to avoid
      // IndexOutOfBoundsException.
      if (jobPageSize <= jobPrevPageSize) {
        jobPage
      } else {
        1
      }
    }
    val currentTime = System.currentTimeMillis()

    try {
      new JobPagedTable(
        store,
        jobs,
        tableHeaderId,
        jobTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "jobs", // subPath
        parameterOtherTable,
        killEnabled,
        currentTime,
        jobIdTitle,
        pageSize = jobPageSize,
        sortColumn = jobSortColumn,
        desc = jobSortDesc
      ).table(page)
    } catch {
      case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering job table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val appInfo = store.applicationInfo()
    val startTime = appInfo.attempts.head.startTime.getTime()
    val endTime = appInfo.attempts.head.endTime.getTime()

    val activeJobs = new ListBuffer[v1.JobData]()
    val completedJobs = new ListBuffer[v1.JobData]()
    val failedJobs = new ListBuffer[v1.JobData]()

    store.jobsList(null).foreach { job =>
      job.status match {
        case JobExecutionStatus.SUCCEEDED =>
          completedJobs += job
        case JobExecutionStatus.FAILED =>
          failedJobs += job
        case _ =>
          activeJobs += job
      }
    }

    val activeJobsTable =
      jobsTable(request, "active", "activeJob", activeJobs, killEnabled = parent.killEnabled)
    val completedJobsTable =
      jobsTable(request, "completed", "completedJob", completedJobs, killEnabled = false)
    val failedJobsTable =
      jobsTable(request, "failed", "failedJob", failedJobs, killEnabled = false)

    val shouldShowActiveJobs = activeJobs.nonEmpty
    val shouldShowCompletedJobs = completedJobs.nonEmpty
    val shouldShowFailedJobs = failedJobs.nonEmpty

    val appSummary = store.appSummary()
    val completedJobNumStr = if (completedJobs.size == appSummary.numCompletedJobs) {
      s"${completedJobs.size}"
    } else {
      s"${appSummary.numCompletedJobs}, only showing ${completedJobs.size}"
    }

    val schedulingMode = store.environmentInfo().sparkProperties.toMap
      .get("spark.scheduler.mode")
      .map { mode => SchedulingMode.withName(mode).toString }
      .getOrElse("Unknown")

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          <li>
            <strong>User:</strong>
            {parent.getSparkUser}
          </li>
          <li>
            <strong>Total Uptime:</strong>
            {
              if (endTime < 0 && parent.sc.isDefined) {
                UIUtils.formatDuration(System.currentTimeMillis() - startTime)
              } else if (endTime > 0) {
                UIUtils.formatDuration(endTime - startTime)
              }
            }
          </li>
          <li>
            <strong>Scheduling Mode: </strong>
            {schedulingMode}
          </li>
          {
            if (shouldShowActiveJobs) {
              <li>
                <a href="#active"><strong>Active Jobs:</strong></a>
                {activeJobs.size}
              </li>
            }
          }
          {
            if (shouldShowCompletedJobs) {
              <li id="completed-summary">
                <a href="#completed"><strong>Completed Jobs:</strong></a>
                {completedJobNumStr}
              </li>
            }
          }
          {
            if (shouldShowFailedJobs) {
              <li>
                <a href="#failed"><strong>Failed Jobs:</strong></a>
                {failedJobs.size}
              </li>
            }
          }
        </ul>
      </div>

    var content = summary
//    content ++= makeTimeline(activeJobs ++ completedJobs ++ failedJobs,
//      store.executorList(false), startTime)

    if (shouldShowActiveJobs) {
      content ++=
        <span id="active" class="collapse-aggregated-activeJobs collapse-table"
            onClick="collapseTable('collapse-aggregated-activeJobs','aggregated-activeJobs')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Active Jobs ({activeJobs.size})</a>
          </h4>
        </span> ++
        <div class="aggregated-activeJobs collapsible-table">
          {activeJobsTable}
        </div>
    }
    if (shouldShowCompletedJobs) {
      content ++=
        <span id="completed" class="collapse-aggregated-completedJobs collapse-table"
            onClick="collapseTable('collapse-aggregated-completedJobs','aggregated-completedJobs')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Completed Jobs ({completedJobNumStr})</a>
          </h4>
        </span> ++
          <div class="form-inline">
            <script src={UIUtils.prependBaseUri(request, "/static/search-utils.js")}></script>
            <div class="bs-example" data-example-id="simple-form-inline">
              <div class="form-group">
                <div class="input-group">
                  Search:
                  <input type="text" class="form-control" id="search" oninput="onSearchStringChange('job-[0-9]+')"></input>
                </div>
              </div>
            </div>
          </div> ++
        <div class="aggregated-completedJobs collapsible-table">
          {completedJobsTable}
        </div>
    }
    if (shouldShowFailedJobs) {
      content ++=
        <span id ="failed" class="collapse-aggregated-failedJobs collapse-table"
            onClick="collapseTable('collapse-aggregated-failedJobs','aggregated-failedJobs')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Failed Jobs ({failedJobs.size})</a>
          </h4>
        </span> ++
      <div class="aggregated-failedJobs collapsible-table">
        {failedJobsTable}
      </div>
    }

    val helpText = """A job is triggered by an action, like count() or saveAsTextFile().""" +
      " Click on a job to see information about the stages of tasks inside it."

    UIUtils.headerSparkPage(request, "Spark Jobs", content, parent, helpText = Some(helpText))
  }

}