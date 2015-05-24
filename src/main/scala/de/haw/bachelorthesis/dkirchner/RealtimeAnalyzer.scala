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

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

package de.haw.bachelorthesis.dkirchner {

import java.io.{FileInputStream, ObjectInputStream}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}

/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object RealtimeAnalyzer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("Model Builder")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)


    val ois = new ObjectInputStream(new FileInputStream("/tmp/tfidf"))
    val scores = ois.readObject.asInstanceOf[Vector]
    ois.close
    // (4) print the object that was read back in
    //scores.take(100).foreach(vector =>
    //println("After: Value for \"Spark\" " + vector.apply(hashingTF.indexOf("Spark".toLowerCase))))

    //val hashTags = {
    //  stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    //}

    val hashingTF = new HashingTF()

    val scoredTweets = {
      stream.map(status => (
        status.getText.split(" ") // TODO: bessere filter?
          .map(word =>
            scores.apply(hashingTF.indexOf(word.toLowerCase))).reduce(_ + _)
          / status.getText().split(" ").length //TODO optimieren, schoener machen
        , status)
      ).transform(_.sortByKey())
    }

    scoredTweets.foreachRDD(rdd => {
      println("Next RDD")
      rdd.take(10).foreach { elem => {
        if (elem._1 > 0)
          println("\nScore: " + elem._1 + "\nText:\n" + elem._2.getText)
      }}
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