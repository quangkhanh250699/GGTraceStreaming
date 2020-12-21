package com.hust.logstream.streaming.machine

case class Machine(time: Long,
                   machineId: Int,
                   eventType: Int,
                   platformId: String,
                   cpus: Float,
                   memory: Float)
