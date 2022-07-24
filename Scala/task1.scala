import org.apache.spark.SparkContext
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import java.io.PrintWriter
import scala.collection.mutable
import java.io._

object task1 {
  def main(args : Array[String]): Unit ={
    val map = new mutable.LinkedHashMap[String, Any]()
    val sc = new SparkContext("local[*]", "Task 1")
    sc.setLogLevel("WARN")
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.map{
      row => val json_row = parse(row)
        (compact(json_row \ "business_id"), compact(json_row \ "user_id"), compact(json_row \ "date"))
    }

    //Q1 - Total number of reviews
    val review_count = rdd2.count()
    println("Review Count"+review_count)


    //Q2 - Numbers of reviews in 2018
    val rdd3 = rdd2.filter(x =>x._3.substring(1,5).equals("2018"))
    val review_count_2018 = rdd3.count()
    println("Number of reviews 2018: " + review_count_2018)


    //Q3 - Number of distinct Users
    val rdd4 = rdd2.map(x => (x._2, 1))
    val distinctUser = rdd4.distinct().count()
    println("Distinct User: "+distinctUser)


    //Q4 - Top 10 users who wrote largest number of reviews
    val userRDD = rdd4.reduceByKey(_+_).sortByKey().sortBy(x=> -x._2)
    val userArr = userRDD.take(10)
    val userList: Array[List[Any]] = new Array[List[Any]](userArr.length)
    for(i<-0 until userArr.length){
      userList(i) = userArr(i).productIterator.toList
    }


    //Q5 - Distinct number of business
    val rdd5 = rdd2.map(x => (x._1, 1))
    val distinctBusiness = rdd5.distinct().count()
    println("Number of Distinct Business: "+distinctBusiness)


    //Q6 - Top 10 Business
    val businessRDD = rdd5.reduceByKey(_+_).sortByKey().sortBy(x=> -x._2)
    val businessArr = businessRDD.take(10)
    val businessList: Array[List[Any]] = new Array[List[Any]](businessArr.length)
    for(i<-0 until businessArr.length){
      businessList(i) = businessArr(i).productIterator.toList
    }
    map += ("n_review"-> review_count)
    map += ("n_review_2018"->review_count_2018)
    map += ("n_user"->distinctUser)
    map += ("top10_user"->userList)
    map += ("n_business"->distinctBusiness)
    map += ("top10_business"->businessList)

    implicit val formats = org.json4s.DefaultFormats
    val writer = new PrintWriter(new File(args(1)))
    writer.write(Serialization.write(map))
    writer.close()

  }
}
