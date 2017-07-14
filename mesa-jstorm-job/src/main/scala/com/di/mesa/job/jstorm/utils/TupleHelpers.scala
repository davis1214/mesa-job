package com.di.mesa.job.jstorm.utils

import backtype.storm.Constants
import backtype.storm.tuple.Tuple

object TupleHelpers {
 
  def isTickTuple(tuple: Tuple): Boolean = {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)
  }
  
}
