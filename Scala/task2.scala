import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import java.io.{File, PrintWriter}
import scala.collection.mutable

object task2 {
  def main(args : Array[String]): Unit = {
    val map_default = new mutable.LinkedHashMap[String, Any]()
    val map_customized = new mutable.LinkedHashMap[String, Any]()
    val map = new mutable.LinkedHashMap[String, Any]()
    val sc = new SparkContext("local[*]", "Task 1")
    sc.setLogLevel("WARN")
    val startTime1 = System.currentTimeMillis()
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.map {
      row =>
        val json_row = parse(row)
        (compact(json_row \ "business_id"), compact(json_row \ "user_id"), compact(json_row \ "date"))
    }
    val rdd3 = rdd2.map(x => (x._1, 1))
    val businessRDD1 = rdd3.reduceByKey(_+_).sortByKey().sortBy(x=> -x._2)
    val endTime1 = System.currentTimeMillis()
    val duration1 = (endTime1 - startTime1) / 1000
    val numPartitions1 = rdd1.getNumPartitions
    val partitionArr1 = rdd1.glom().map(x=> x.length).collect()

    map_default += ("n_partition"->numPartitions1)
    map_default += ("n_items" ->partitionArr1)
    map_default += ("exe_time" -> duration1)

    //Second Part - With Custom Partitions
    val num_partitions = args(2).toInt
    val startTime2 = System.currentTimeMillis()
    val rdd4 = sc.textFile(args(0),num_partitions)
    val rdd5 = rdd4.map {
      row =>
        val json_row = parse(row)
        (compact(json_row \ "business_id"), compact(json_row \ "user_id"), compact(json_row \ "date"))
    }
    val rdd6 = rdd5.map(x => (x._1, 1))
    val businessRDD2 = rdd6.reduceByKey(_+_).sortByKey().sortBy(x=> -x._2)
    val endTime2 = System.currentTimeMillis()
    val duration2 = (endTime2 - startTime2) / 1000
    println(duration2)
    val numPartitions2 = rdd4.getNumPartitions
    val partitionArr2 = rdd4.glom().map(x=> x.length).collect()
    map_customized += ("n_partition"->numPartitions2)
    map_customized += ("n_items" ->partitionArr2)
    map_customized += ("exe_time" -> duration2)
    map += ("default"-> map_default)
    map += ("customized"-> map_customized)

    implicit val formats = org.json4s.DefaultFormats
    val writer = new PrintWriter(new File(args(1)))
    writer.write(Serialization.write(map))
    writer.close()
  }
}
