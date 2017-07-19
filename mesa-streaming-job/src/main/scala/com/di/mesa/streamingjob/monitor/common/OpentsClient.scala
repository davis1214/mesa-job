package com.di.mesa.streamingjob.monitor.common

import com.sun.jersey.api.client.Client
import java.io.BufferedReader
import java.io.InputStreamReader

object OpentsClient {

  val client = Client.create()

  import javax.ws.rs.core.MediaType
  import com.sun.jersey.api.client.ClientResponse

  val queryBuilder = client.resource("http://localhost:4242/api/query")
    .accept(MediaType.APPLICATION_JSON_TYPE)
    .`type`(MediaType.APPLICATION_FORM_URLENCODED_TYPE)

  val putBuilder = client.resource("http://localhost:4242/api/put?summary=true")
    .accept(MediaType.APPLICATION_JSON_TYPE)
    .`type`(MediaType.APPLICATION_FORM_URLENCODED_TYPE)

  def put(jsonStr: String): String = {
    val response = putBuilder.post(classOf[ClientResponse], jsonStr)

    if (response.getStatus == 200) {
      println("post successed")
      val result = response.getEntity(classOf[String])
      result
    } else {
      response.getEntity(classOf[String])
    }
  }

  def query(jsonStr: String): String = {
    val response = queryBuilder.post(classOf[ClientResponse], jsonStr)

    if (response.getStatus == 200) {
      println("post successed")
      val result = response.getEntity(classOf[String])
      result
    } else {
      response.getEntity(classOf[String])
    }
  }


}
