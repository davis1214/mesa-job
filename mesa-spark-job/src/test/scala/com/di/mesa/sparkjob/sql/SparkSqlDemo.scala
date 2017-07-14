package com.di.mesa.sparkjob.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 17/7/10.
  */
object SparkSqlDemo {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
        .master("local")
      .appName("spark sql app name ")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()



    import spark.implicits._


    val df = spark.read.json("examples/people.json")

    df.show()





  }


}
