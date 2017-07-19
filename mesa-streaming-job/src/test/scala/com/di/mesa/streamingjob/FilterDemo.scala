package com.di.mesa.streamingjob

/**
  * Created by Administrator on 16/6/6.
  */
object FilterDemo extends App {

  def ruleFilterContains(str: String, line: String): Boolean = {

    def grepFun(): Boolean = {
      str.split("&&&").foreach(one => {
        if (line.indexOf(one) > -1) {
          return true
        }
      })
      false
    }

    def indexFun(): Boolean = {
      str.split("&&").foreach(one => {
        if (line.indexOf(one) == -1) {
          return false
        }
      })
      true
    }

    val result = if (str.contains("&&")) {
      indexFun()
    }
    else if (str.contains("&&&")) {
      grepFun()
    } else {
      if (line.indexOf(str) == -1) false else true
    }
    result
  }


  //val str = "aaaa&&bbbb"
  val str = "time="
  val line = "aaa=tic&c=23&time=140238295&bbb=take"

  println("result:" + ruleFilterContains(str,line))

}
