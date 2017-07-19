package util

import org.jboss.netty.util.internal.ConcurrentHashMap
import java.util.logging.SimpleFormatter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong

object FileReaders {

  def main(args: Array[String]): Unit = {

    val smf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val map: ConcurrentHashMap[String, AtomicLong] = new ConcurrentHashMap

    val a = "/Users/Administrator/Documents/test/context.txt"
    val file = "/Users/Administrator/Documents/test/trace_log_1"

    val bufferedReader = scala.io.Source.fromFile(file).bufferedReader()

    val str = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null)

    def getTimeStr(time: Long): String = smf.format(new Date(time))

    def getTime(line: String): Long = line.split(" ").apply(0).toLong

    var cont: Long = 0

    str.foreach { line =>
      // println(line)

      if (line.contains("service/query_merchant_info.do")) {

        cont += 1

        if (cont % 100 == 0) {
          println(map)
        }

        val time = getTime(line)

        val timestr = getTimeStr(time)

        if (map.containsKey(timestr)) {

          map.get(timestr).getAndIncrement

        } else {
          map.put(timestr, new AtomicLong(1))
        }
      }
    }


  }

}