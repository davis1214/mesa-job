package com.di.mesa.sparkjob

/**
  * Created by Administrator on 17/7/4.
  */

import org.scalatest.{FunSpec, ShouldMatchers}

class AlbumTest extends FunSpec with ShouldMatchers {


  class Artist(val firstName: String, val lastName: String)

  class Album(val title: String, val year: Int, val artist: Artist)


  describe("An Album") {
    it("can add an Artist object to the album") {
      val album = new Album("Thriller", 1981, new Artist("Michael", "Jackson"))
      album.artist.firstName should be("Michael")
    }
  }
}
