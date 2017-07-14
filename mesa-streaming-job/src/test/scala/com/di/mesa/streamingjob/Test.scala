package com.di.mesa.streamingjob

/**
  * Created by Administrator on 17/7/14.
  */
object Test extends App {


  val userID = Map((1 to 10).map(_ -> .01): _*)


  userID.foreach(println)


  println("--->")


  val a = (1 to 10).map(_ -> .01)

  a.foreach(println)


  val v = (1 to 10).map(_ -> .01): _*


}
