package com.di.mesa.job.jstorm.utils

object StringTools {
	
  def isEmpty(s: String): Boolean = {
		Option(s) match {
			case Some(s) => {
				s.trim().isEmpty
			}
			case _ => {
				true
			}
		}
	}
}
