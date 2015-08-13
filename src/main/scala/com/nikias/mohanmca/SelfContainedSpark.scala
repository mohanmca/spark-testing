package com.nikias.mohanmca

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SelfContainedSpark {
  def main(args: Array[String]) {
    val logFile = "C:/Users/mohan/git/spark-testing/README.md" // Should be some file on your system
    //    val conf = new SparkConf("local").setAppName("Simple Application")
    //    val sc = new SparkContext(conf)
    //    lazy val sc = new SparkContext(sparkConf)
    lazy val sc = new SparkContext("local","Standalone")
    
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}