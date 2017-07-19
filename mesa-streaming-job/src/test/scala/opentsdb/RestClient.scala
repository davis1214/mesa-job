package com.demo.spark

import com.sun.jersey.api.client.Client

/**
 * Created by geovaniemarquez on 1/17/15.
 */
object RestClient {

  val hostIP = "PLACE_HOS_IP_HERE"
  val hostTSDPort = "PLACE_TSD_PORT_HERE"

  val tsd = "http://104.131.184.19:4242/api/put"
  val restClient = Client.create()
  val webResource = restClient.resource(tsd)

}
