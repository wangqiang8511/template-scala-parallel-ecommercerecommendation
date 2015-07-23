package org.template.ecommercerecommendation

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      viewEvents = trainingData.viewEvents,
      buyEvents = trainingData.buyEvents),
      dislikeEvents = trainingData.dislikeEvents),
      wishEvents = trainingData.wishEvents)
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
  val buyEvents: RDD[BuyEvent],
  val dislikeEvents: RDD[DislikeEvent],
  val wishEvents: RDD[WishEvent],
) extends Serializable
