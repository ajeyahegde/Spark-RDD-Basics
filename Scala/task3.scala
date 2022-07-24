import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import scala.collection.mutable
import scala.util.Sorting

object task3 {
  def main(args : Array[String]): Unit = {
    val map_default = new mutable.LinkedHashMap[String, Any]()
    val map_customized = new mutable.LinkedHashMap[String, Any]()
    val map = new mutable.LinkedHashMap[String, Any]()
    val sc = new SparkContext("local[*]", "Task 1")
    sc.setLogLevel("WARN")
    val reviewFilepath = args(0)
    val businessFilepath = args(1)
    val outputFilepath1 = args(2)
    val outputFilepath2 = args(3)
    val startTime = System.currentTimeMillis()
    val rdd1 = sc.textFile(reviewFilepath)
    val businessStars = rdd1.map {
      row =>
        val json_row = parse(row)
        (compact(json_row \ "business_id"), compact(json_row \ "stars"))
    }
    val rdd2 = sc.textFile(businessFilepath)
    val businessCity = rdd2.map {
      row =>
        val json_row = parse(row)
        (compact(json_row \ "business_id"), compact(json_row \ "city"))
    }

    val rdd3 = businessStars.join(businessCity)
    val rdd4 = rdd3.map(x => (x._2._2, x._2._1.toFloat))
    val rdd5 = rdd4.groupByKey().mapValues{x => x.sum / x.size}
    val rdd6 = rdd5.sortByKey()
    val endTime = System.currentTimeMillis()
    val commonDuration = (endTime-startTime)/1000

    val startTime1 = System.currentTimeMillis()
    val ratingArr = rdd6.collect().toArray.sortBy(_._2)(Ordering[Float].reverse)
    for(i<-0 until 10){
      println(ratingArr(i))
    }
    val endTime1 = System.currentTimeMillis()
    val duration1 = commonDuration + ((endTime1-startTime1)/2)

    val startTime2 = System.currentTimeMillis()
    rdd6.sortBy(x=> -x._2).take(10).foreach{println _}
    val endTime2 = System.currentTimeMillis()
    val duration2 = commonDuration + ((endTime2-startTime2)/2)

    map += ("m1"->duration1)
    map += ("m2"->duration2)
    map += ("reason"->"")

    val file = new File(outputFilepath1)
    val bufferedWriter = new BufferedWriter(new FileWriter(file))
    bufferedWriter.write("city,stars\n")
    for(i<-0 until ratingArr.length){
      bufferedWriter.write(ratingArr(i)._1.substring(1,ratingArr(i)._1.length-1)+","+ratingArr(i)._2+"\n")
    }
    bufferedWriter.close()

    implicit val formats = org.json4s.DefaultFormats
    val writer = new PrintWriter(new File(outputFilepath2))
    writer.write(Serialization.write(map))
    writer.close()
  }
}
