package com.di.mesa.sparkjob.collections

/**
  * Created by Administrator on 17/7/5.
  */
object JoinsTest extends App {


  val a = Array(("a", "aaaaaa"), ("b", "bbbbb"), ("c", "cccccc"))
  val b = Array(("a", "a->aaaaaa"), ("b", "b->bbbbb"), ("c", "c->cccccc"))


  val aa = a.map(a => a)
  val bb = b.map(b => b)

  aa.intersect(bb).foreach(println)


}
