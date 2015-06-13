/*
 * This file is part of my bachelor thesis.
 *
 * Copyright 2015 Daniel Kirchner <daniel.kirchner1@haw-hamburg.de>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */

package de.haw.bachelorthesis.dkirchner {

import java.io.{FileInputStream, ObjectInputStream}
import java.util.Calendar
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vector}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
 * Analyzes a stream of tweets by scoring each by relevant words
 * according to an externally fed relevance vector
 *
 *
 */
object RealtimeAnalyzer {
  // Minimum score for a tweet to be considered relevant
  val minScore: Double = -1.0

  // local file system path to load the feature vector from
  private val modelPath: String = "/tmp/tfidf"

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RealtimeAnalyzer <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val sparkConf = new SparkConf().setAppName("Model Builder")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None)
    var scores: Vector = null

    try {
      val ois = new ObjectInputStream(new FileInputStream(modelPath))
      scores = ois.readObject.asInstanceOf[Vector]
      ois.close()
    } catch {
      case e: Exception =>
        println("Fehler beim Einlesen der Feature-Vektors: " + e)
        ssc.stop()
        System.exit(1)
    }

    val broadcastScores = ssc.sparkContext.broadcast(scores)

    val hashingTF = new HashingTF(1 << 20)

    // Split text sting into single words
    val splitTweets = {
      stream.map(status =>
        (status.getText.split(" "), status)
      )
    }

    // calculate score for each word, then sum the scores and normalize
    val scoredTweets = {
      splitTweets.map(splitTweet => {
        (splitTweet._1.map(word =>
          broadcastScores.value.apply(
             hashingTF.indexOf(word.toLowerCase
               .replaceAll("[^a-zA-Z0-9]", " ")))
         ).sum./(splitTweet._2.getText.split(" ").length),
          splitTweet._2
          )}
      )
    }

    val tweetSink = new StringBuilder
    tweetSink.append(Calendar.getInstance.getTime + "," + Calendar.getInstance.getTimeZone + "\n")
    tweetSink.append("Score,Message Content")
    scoredTweets.foreachRDD(rdd => {
      //println("Next RDD")
      rdd.collect().foreach { elem => {
        if (elem._1 > minScore)
          //println("\nScore: " + elem._1 + "\nText:\n" + elem._2.getText + "\n")
          tweetSink.append(elem._1 + ",\"\"\"" + elem._2.getText.replace("\n", " ") + "\"\"\"\n")
      }}
      println(tweetSink)
      tweetSink.clear()
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
}