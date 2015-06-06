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
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

/**
 * Analyzes a stream of tweets by scoring each by relevant words
 * according to an externally fed relevance vector
 *
 *
 */
object RealtimeAnalyzer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RealtimeAnalyzer <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    //val filters = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Debug
    /*
    println("consumer key: " + consumerKey + "\n" +
    "consumer secret: " + consumerSecret  + "\n" +
    "access token: " + accessToken + "\n" +
    "access token secret: " + accessTokenSecret)

    System.exit(0)
    */

    val sparkConf = new SparkConf().setAppName("Model Builder")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None)

    val ois = new ObjectInputStream(new FileInputStream("/tmp/tfidf"))
    val scores = ois.readObject.asInstanceOf[Vector]
    ois.close()

    val hashingTF = new HashingTF(1 << 20)

    val scoredTweets = {
      stream.map(status => (
        status.getText.split(" ") // TODO: bessere filter?
          .map(word =>
            scores.apply(hashingTF.indexOf(word.toLowerCase))).reduce(_ + _)
          ./(status.getText.split(" ").length)
        , status)
      )//.transform(_.sortByKey())
    }

    val tweetSink = new StringBuilder
    scoredTweets.foreachRDD(rdd => {
      println("Next RDD")
      rdd.collect.foreach { elem => {
        if (elem._1 > 1)
          tweetSink.append("\nScore: " + elem._1 + "\nText:\n" + elem._2.getText + "\n")
      }}
      println("Relevant Tweets: " + tweetSink)
        //case (score, status) => {
        //if (score > 0)
        //  println("\n###### Score " + score + "######\n" + status.getText)
    })



    /*val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })*/

    ssc.start()
    ssc.awaitTermination()
  }
}
}
/*
object ScalaApp {
  val my_spark_home = "/home/daniel/projects/spark-1.1.0"

  def main(args: Array[String]): Unit = {
    println("Hello wrld")

    val logFile = my_spark_home + "/README.md"

    val conf = new SparkConf().setAppName("ScalaApp")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("lines with a: %s, lines with b: %s".format(numAs, numBs))
    val parList1 = sc.parallelize(List(1,2,3,4,5,6))
    val parList2 = sc.parallelize(List(5,6,7,8,9,10))
    val str1 = "created RDD1: %s".format(parList1.collect().deep.mkString(" "))
    val str2 = "created RDD2: %s".format(parList2.collect().deep.mkString(" "))
    val str3 = "Number of RDD1: %s".format(parList1.count())
    val str4 = "Intersection: %s".format(parList1.intersection(parList2).collect().deep.mkString(" "))
    val str5 = "Intersection: %s".format(parList1.cartesian(parList2).collect().deep.mkString(" "))

    println(str1)
    println(str2)
    println(str3)
    println(str5)
  }
}
*/