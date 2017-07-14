package com.di.mesa.sparkjob.scripts

import com.di.mesa.sparkjob.util.SparkCommon
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 17/7/10.
  */
object SparkScriptDemo {


  private implicit def string2Boolean(str: String): Boolean = str.toBoolean

  def testA(sc: SparkContext): Unit = {

    case class Juice(volumn: Int) {
      def add(j: Juice): Juice = Juice(volumn + j.volumn)
    }

    case class Fruit(kind: String, weight: Int) {
      def makeJuice: Juice = Juice(weight * 100)
    }

    val apple1 = Fruit("apple", 5)
    val apple2 = Fruit("apple", 8)
    val orange1 = Fruit("orange", 10)

    val fruit = sc.parallelize(List(("apple", apple1), ("orange", orange1), ("apple", apple2)))


    val juice = fruit.combineByKey(
      f => f.makeJuice,
      (j: Juice, f) => j.add(f.makeJuice),
      (j1: Juice, j2: Juice) => j1.add(j2)
    )

    juice.collect().foreach(println)



    //    val scores = sc.parallelize(List(("chinese", 88.0), ("chinese", 90.5), ("math", 60.0), ("math", 87.0)))
    //    val avg = scores.combineByKey(
    //      (v) => (v, 1),
    //      (acc: (Float, Int), v) => (acc._1 + v, acc._2 + 1),
    //      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    //    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }


    var rdd1 = sc.makeRDD(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2), ("C", 1)))

    //    rdd1.combineByKey(
    //      (v: Int) => v + "_",
    //      (c: String, v: Int) => c + "@" + v,
    //      (c1: String, c2: String) => c1 + "$" + c2
    //    ).collect.foreach(println)


    rdd1.combineByKey(
      (v: Int) => v + "_",
      (c: String, v: Int) => c + "@" + v,
      (c1: String, c2: String) => c1 + "->" + c2
    ).collect().foreach(println)


  }

  def main(args: Array[String]) {


    //    val Array(isLocalMode) = Array(args(00))


    val sc: SparkContext = SparkCommon.getLocalSparkContext


    val sourceRdd = sc.parallelize(1 to 50)

    val fmtRdd = sourceRdd.map(f => if (f % 2 == 0) (f % 2, (f % 3 * 2, 1)) else (f % 4, (f % 3 * 2, 1)))

    //fmtRdd.collect().foreach(println)


    //printOne(fmtRdd)
    //printTwo(fmtRdd)


    testA(sc);


    //    val gRdd = fmtRdd.groupBy(f => f._1)
    //    gRdd.collect().foreach(println)


  }

  def printTwo(fmtRdd: RDD[(Int, (Int, Int))]): Unit = {
    val parsedRdd = fmtRdd.map(f => {
      (f._1 + "@" + f._2._1, f._2._2)
    }).groupByKey.map(f => {

      val key = f._1.split("@")
      (key(0), (key(1), f._2.size))
    })

    /**
      * (1,(4,4))
      * (1,(0,4))
      * (0,(0,8))
      * (1,(2,5))
      * (0,(4,9))
      * (0,(2,8))
      * (3,(2,4))
      * (3,(0,4))
      * (3,(4,4))
      */
    //parsedRdd.collect().foreach(println)


    parsedRdd.reduceByKey((pre, after) => {

      if (pre._1.equals(after._1)) {
        (pre._1, pre._2 + after._2)
      }
      else {
        (pre._1 + after._1, pre._2 + after._2)
      }

    }).collect().foreach(println)


    /**
      * (0,CompactBuffer((0,8), (4,9), (2,8)))
      * (3,CompactBuffer((2,4), (0,4), (4,4)))
      * (1,CompactBuffer((4,4), (0,4), (2,5)))
      */
    //parsedRdd.groupByKey().collect().foreach(println)

    /**
      * (0,(0,25))
      * (3,(2,12))
      * (1,(4,13))
      */
    //    parsedRdd.reduceByKey((pre, after) => {
    //      (pre._1 , pre._2 + after._2)
    //    }).collect().foreach(println)


    /**
      * (0,(0@4@2,25))
      * (3,(2@0@4,12))
      * (1,(4@0@2,13))
      */
    //    parsedRdd.reduceByKey((pre, after) => {
    //      (pre._1 + "@" + after._1, pre._2 + after._2)
    //    }).collect().foreach(println)


  }


  def printOne(fmtRdd: RDD[(Int, (Int, Int))]): Unit = {
    val parsedRdd = fmtRdd.map(f => {
      (f._1 + "@" + f._2._1, f._2._2)
    }).groupByKey.map(f => {
      (f._1, f._2.size)
    })

    parsedRdd.collect().foreach(println)

  }
}
