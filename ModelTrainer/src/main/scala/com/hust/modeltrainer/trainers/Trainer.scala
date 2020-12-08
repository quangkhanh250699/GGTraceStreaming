package com.hust.modeltrainer.trainers

trait Trainer {

  def getDataPath: String = ???

  def getModelPath: String = ???

  def train: Unit = ???

}
