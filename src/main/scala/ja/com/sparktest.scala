package ja.com

import ja.conf.JobSparkConf

/**
  * Created by Ja on 07/06/2017.
  */
object sparktest {

  def main(args: Array[String]): Unit = {
     val rdd1 =  JobSparkConf.sc.textFile("c:\\spark\\CDR.csv")

    //***************************************

    // way 1
    import JobSparkConf.sqlContext.implicits._
    val df1 = rdd1.map(x => (
      x.split(",")(0), x.split(",")(1).toInt, x.split(",")(2),x.split(",")(3))
    ).toDF("loc", "duration", "phone", "errorcode")

    val dffilter = df1.filter(df1("errorcode").equalTo("0x829F08"))

    val dffiltergroup = dffilter.groupBy($"phone").sum("duration").toDF("grpPhone", "maxduration")

     dffiltergroup.sort($"maxduration" desc).limit(10).show(11)

    //   val ipFile = sc.textFile("/Users/janaidu/Documents/Repo040216/spark_master-master/sparkmaster/CDR.csv")
    //way 2
    val df_sql = rdd1.map(x => (x.split(",")(0), x.split(",")(1).toInt, x.split(",")(2),x.split(",")(3))).toDF("loc", "duration", "phone", "errorcode")
    df_sql.registerTempTable("t1")
    val df2 = JobSparkConf.sqlContext.sql("Select phone, sum(duration) as sd from t1 where errorcode='0x829F08' group by phone order by sum(duration) desc  limit 10")
    df2.show(10)

    //***************************************



    def fetchElement(line: String, loc: Int) = {    //
      val s = line.split(",")
      s(loc)
    }

    val arr = "1,2,3,4".split(",")
    println(arr(0))
                                                                    // 0 1 2 3
    import JobSparkConf.sqlContext.implicits._          // toDF("loc", "duration", "phone", "errorcode")
    val countByNumber = rdd1.map(line =>  (line.split(",")(2), line.split(",")(3), line.split(",")(1))).filter(x => x._2.equals("0x829F08"))
    countByNumber.take(20).foreach(println)
    val countByNumber1 = countByNumber.map( x => (x._1, x._3.toInt)).reduceByKey(_+_)
    countByNumber1.take(20).foreach(println)
    val countByNumber_sortedDsc = countByNumber1.sortBy(_._2, false)


    countByNumber_sortedDsc.toDF().show(10)

                                             // 0              1          2           3
    /*                                         // toDF("loc", "duration", "phone", "errorcode")
    val countByNumber_2 = rdd1.map(line =>  (line.split(",")(2), line.split(",")(3), line.split(",")(1).toInt)).filter(x => x._2.equals("0x829F08"))
    val grprdd = countByNumber_2.groupBy(x => (x._1, x._3))
    val x1 = grprdd.map(x => (x._1._1, x._1._2)).reduceByKey(_+_).sortBy(x => x._2, false).take(10).foreach(println)
*/
      //.sortBy(x=> x._1).take(10).foreach(println)
   // grprdd.sortBy(x => x._2.toList(2), false) .take(10).foreach(println)

/*
    // Bazic groupBy example in scala
      val x = JobSparkConf.sc.parallelize(Array("Joseph", "Jimmy", "Tina", "Thomas", "James", "Cory", "Christine", "Jackeline", "Juan"), 3)
      x.toDF().show(10)*/
}
}
