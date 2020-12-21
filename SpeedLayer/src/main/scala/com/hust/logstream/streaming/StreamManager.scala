package com.hust.logstream.streaming

import com.hust.logstream.streaming.stream.Stream

abstract class StreamManager extends Serializable {

  protected val stream: Stream

  def start: Unit = ???
  def getStatistics: Unit = ???
  def showStatistics: Unit = ???
}
