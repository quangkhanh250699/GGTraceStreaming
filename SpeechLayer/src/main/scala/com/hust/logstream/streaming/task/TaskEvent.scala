package com.hust.logstream.streaming.task

case class TaskEvent(val id: Int,
                     val time: Int,
                     val missingInfo: Int,
                     val jobId: Int,
                     val taskIndex: Int,
                     val machineId: Int,
                     val eventType: Int,
                     val user: String,
                     val schedulingClass: Int,
                     val priority: Int,
                     val cpuRequest: Float,
                     val memoryRequest: Float,
                     val diskSpaceRequest: Float,
                     val differentMachinesRestriction: Int) extends Task
