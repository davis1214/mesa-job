package com.di.mesa.job.jstorm.utils

object MesaFilter {

  val backUrl = List(
    "getNotificationCount",
    "getGuessWithCluster.do",
    "checkNewRecommend.do",
    "listCommodityKind_v3.do",
    "notify.do",
    "uploadStoreProducts.do",
    "delProduct.do",
    "uploadStoreShops.do",
    "delShop.do",
    "deleteMystreet.do",
    "getFollowShopsNewStatus.do",
    "splashPage.do",
    "hasNewSubscribe.do",
    "getConfig.do",
    "checkNewRecommend.do",
    "anonymousRegist.do",
    "uploadDeviceInfo.do",
    "update/")
  val backStr = backUrl.mkString(",")

  def isBackRequest(url: String): Boolean = {
    if (backStr.indexOf(url) >= 0) true else false
  }
}
