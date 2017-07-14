package com.di.mesa.sparkjob.config

object Config {

  // 访问本地MySQL服务器，通过3306端口访问mysql数据库
  val url = "jdbc:mysql://localhost:3306/mesa_report?characterEncoding=utf8"
  val driver = "com.mysql.jdbc.Driver"
  val username = "mesa_report"
  val password = "mesa_report"


  //"jdbc:mysql://localhost:3306/test?user=root&password=root"
  def getJdbcUrl = url.concat("&user=").concat(username).concat("&password=").concat(password)

}
