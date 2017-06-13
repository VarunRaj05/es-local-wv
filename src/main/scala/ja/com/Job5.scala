package ja.com

import java.io.FileOutputStream
import java.util.regex.Matcher

import ja.com.Common.cdxItem
import ja.conf.JobSparkConf
import org.apache.spark.sql.types.{LongType, StringType}
import org.jsoup.Jsoup

import scala.collection.mutable.ListBuffer

object Job4 {
  case class t(x : Long, y: String)
  case class allLines( linenumber: Long  , isvalidheader:Boolean ,isheader:Boolean, url:String, urltime:String, mime:String , line:String)
  case class finalOutput(ln: String, p_urltime:String, p_mime:String, p_url:String,
                         line:String, title:String, metadata:String,contentLength: String,etag:String,
                         htmldate:String,lastmodifieddate:String,
                         imageData : String, digest: String, indexingTime: String, language: String)
  import java.util.regex.Pattern
  val dataLinePattern = Pattern.compile("(\\d{14})")


  def isCurrentLineIsValidHeader(line:String, lstMimes: List[String] ) : Boolean  = {

    if(lstMimes.exists(line.contains)) {
      val matcher: Matcher = dataLinePattern.matcher(line)
      matcher.find

    }
    else {
      false
    }
  }
  def isCurrentLineIsHeader(line:String) : Boolean  = {
    val valid1 = (line.split(" ").length > 2)
    val matcher: Matcher = dataLinePattern.matcher(line)
    matcher.find && valid1  //call ok

  }

  def main(args: Array[String]): Unit = {       // 2

    val language = "En"
    var arcfile = "/srcfileloc/*.arc"
    var cdxfile = "/srcfileloc/*.cdx"
    var dnsfiles = "/whiltelist/dns_*"
    var username = "edureka"
    // var username = "cloudera"
    //var username = "root"

    //
    //    C:\Users\Ja\Google Drive\srcfile\Note\srcfile\latestarcfiles\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000
    //    C:\Users\Ja\Google Drive\srcfile\Note\srcfile\latestarcfiles\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc
    //    arcfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\Note\\srcfile\\EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.arc"
    //    cdxfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\srcfileloc\\*.cdx"
    //
    if(args.length > 0) {
      val filelocation = args(0)
      if (filelocation.equals("local")) {
        arcfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\Note\\srcfile\\latestarcfiles\\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc\\*.arc"
        cdxfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\Note\\srcfile\\latestarcfiles\\*.cdx"  // we dont need to do anything except that joining side - ok
        dnsfiles = "C:\\Users\\Ja\\Google Drive\\oldfiles\\Note\\srcfile\\SearchWhitelist\\dns_*"

        println(arcfile)
        println(cdxfile)
        println(dnsfiles)
      }
    }

    println(arcfile)
    println(cdxfile)
    println(dnsfiles)

    val output = new FileOutputStream("application.conf")
    System.setProperty("HADOOP_USER_NAME", username)

    // ********************************************

    val txtRDD = JobSparkConf.sc.textFile(cdxfile)
    //val txtRDD = JobSparkConf.sc.textFile("/srcfileloc/*.CDX")
    val rddLines = txtRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    import JobSparkConf.sqlContext.implicits._
    val cdxItems = rddLines.map(x => cdxItem(x.split(" ")(0)
      ,x.split(" ")(1),x.split(" ")(2), x.split(" ")(3),
      x.split(" ")(4),x.split(" ")(5), x.split(" ")(6),
      x.split(" ")(7),x.split(" ")(8),x.split(" ")(9)
    )
    ).toDF()

    // robots.txt, .js and, .json extensions, and any files which returned an HTTP status code of 400
    val filteredCDXitems = cdxItems
      .filter(!(cdxItems("a_origina_url").endsWith("robots.txt")
        || cdxItems("a_origina_url").endsWith(".js")
        || cdxItems("a_origina_url").endsWith(".json")
        ))

    // println(filteredCDXitems)
    // filteredCDXitems.printSchema()

    // filteredCDXitems.show(10,truncate = false)

    // ********************************************


    // s_response_code from 400  to 511
    import ja.com.Common._


    val filteredCDXitems1 = filteredCDXitems
      .filter(!(filteredCDXitems("s_response_code").between("400", "511")))
      .withColumn("New_URL", toGetURLnew(filteredCDXitems("a_origina_url")))

    // toGetURL(txtRDD).toString

    //println(filteredCDXitems1)
    //  filteredCDXitems1.show(10000,truncate = false)
    filteredCDXitems1.registerTempTable("mainT")

    // val inscopeDomains = JobSparkConf.sc.textFile("/whiltelist/dns_*")
    val inscopeDomains = JobSparkConf.sc.textFile(dnsfiles)   //
    val domainsDf =   inscopeDomains.map( x => {
      if(x.indexOf("/") >= 0 )
        x.substring(0,x.indexOf("/")).replaceAll("\"", "")
      else
        x.replaceAll("\"", "")
    }).toDF("domain")


    /*domainsDf.printSchema()
    domainsDf.show(10,truncate = false)*/


    // ********************************************

    println(" line number 120 count 1=" + domainsDf.count())
    domainsDf.registerTempTable("domainT")

    //  domainsDf.show(10)
    val joinedDF = JobSparkConf.sqlContext.sql("select mainT.* from mainT inner join domainT" +
      " ON mainT.New_URL LIKE concat('%',domainT.domain,'%') ")
    joinedDF.registerTempTable("joinedTable")

    // joinedDF.show(100)
    // 20090831084108 - image/jpeg - http://www.biglotteryfund.org.uk/index/newsroom-uk/openevent_08_2.jpg-
    /// Arc file starts here

    println(" line number 134 , count 1=" + joinedDF.count())

    // joinedDF.show(100)

    // *****************************************************

    val txtArcRDD = JobSparkConf.sc.textFile(arcfile)
    //val txtArcRDD = JobSparkConf.sc.textFile("/srcfileloc/EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.arc")
    val indexedArcRDD = txtArcRDD.zipWithIndex
    import JobSparkConf.sqlContext.implicits._
    val mainDF = indexedArcRDD.toDF("line", "linenumber")
    mainDF.registerTempTable("tmp1")
    val query  = "Select tmp1.linenumber + 1 linenumber, tmp1.line from tmp1 " +
      " join (select linenumber from tmp1 where trim(line) = '') as t2 " +
      " on tmp1.linenumber = t2.linenumber + 1 where trim(tmp1.line) <> '' "
    //" left join (select distinct m_mime_type_of_original_document mime from joinedTable) as t3 "
    //" on tmp1.line like concat('%',t3.mime,'%') where trim(tmp1.line) <> '' "


    //   println(" line number 151 , count 1=" + query)


    // *****************************************************
    val dfMimes = joinedDF.select(joinedDF("m_mime_type_of_original_document")).distinct().toDF("mime")
    //  dfMimes.registerTempTable("mimestable")
    //dfMimes.show(100)
    //dfMimes.count()   // TODO : 1
    //val broadcastVar = sc.broadcast(Array(1, 2, 3))
    var lstMimes = new ListBuffer[String]()
    val mlist1 = dfMimes.collect()
    val mlist = mlist1.foreach(x => {
      //  println(x)

      lstMimes += x.getAs[String]("mime")
    })

    // println(" line number 168 , count 1=" + mlist)

    // *****************************************************

    //  val broadcastVar = JobSparkConf.sc.broadcast(lstMimes.toList)

    val f1= JobSparkConf.sqlContext.sql(query).select("linenumber", "line").rdd

    /*val linenumbersBtwSpaces = f1.collect().map( x => x.getAs[Long]("linenumber"))
   // println("line number for space lines")
    // linenumbersBtwSpaces.sorted.take(100).foreach(println)*/

    //println("fk2")
    import java.util.regex.Pattern
    val pattern = Pattern.compile("(\\d{14})")

    val f4 = mainDF.map(x => {
      val ln = x.getAs[Long]("linenumber")
      val line = x.getAs[String]("line")
      var isvalidheader = isCurrentLineIsValidHeader(line, lstMimes.toList)
      var isheader = isCurrentLineIsHeader(line)
      var urltime = ""
      var url = ""
      var mimetype = ""
      if(isvalidheader){
        val arr = line.split(" ")
        url = arr(0)
        urltime = arr(2)
        mimetype = arr(3)
      }

      allLines( ln+1  , isvalidheader ,isheader,url,urltime,mimetype,  line)

    }).toDF().registerTempTable("fk4")
    //  println(" line number 202 , key - values")

    // *****************************************************

    val arcDF = JobSparkConf.sqlContext.sql("select * from fk4 order by linenumber")
    //  arcDF.show(10)
    //joinedTable  String.valueOf
    //val finalDF1 = JobSparkConf.sqlContext.sql("select distinct fk4.*, cast(joinedTable.digest as String) from fk4 " +
    val finalDF1 = JobSparkConf.sqlContext.sql("select distinct fk4.*, joinedTable.digest digest from fk4 " +
      " join joinedTable on trim(joinedTable.b_date) = trim(fk4.urltime)  " +
      " and joinedTable.m_mime_type_of_original_document = trim(fk4.mime) " +
      " and fk4.url like concat('%',joinedTable.New_URL,'%')  " +
      "order by fk4.linenumber").toDF()
    finalDF1.persist()


    //  finalDF1.show(10)

    println("finalDF1 count =" + finalDF1.count())

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.functions._
    //|linenumber|isvalidheader|isheader|url  |urltime       |mime     |line |                                                // String.valueOf()

    var initialDF = JobSparkConf.sqlContext.createDataFrame(JobSparkConf.sc.emptyRDD[Row], finalDF1.schema).withColumn("p_linenumberA", lit(null).cast(LongType))
      .withColumn("p_urltime", lit(null).cast(StringType)).withColumn("p_mime", lit(null).cast(StringType)).withColumn("p_url", lit(null).cast(StringType))


    val finalDF2 = finalDF1.collect().foreach(x => {
      val p_linenumberA = x.getAs[Long]("linenumber")
      val p_urltime = x.getAs[String]("urltime")
      val p_mime = x.getAs[String]("mime")
      val p_url = x.getAs[String]("url")
      val digest = x.getAs[String]("digest")


      // ***************

      val q2  = s"select min(linenumber) ln from fk4 where linenumber > $p_linenumberA and isheader = true"
      val minDF = JobSparkConf.sqlContext.sql(q2).toDF()
      val maxLineNo = minDF.map(t => t.getAs[Long]("ln")).collect().head
      //check this is  in a min

      // val q= s"select *,$p_linenumberA p_linenumberA from fk4 where linenumber >= $p_linenumberA and linenumber < $maxLineNo"
      val q= s"select * from fk4 where linenumber >= $p_linenumberA and linenumber < $maxLineNo"
      val finalDFInner = JobSparkConf.sqlContext.sql(q).toDF()
        .withColumn("digest", lit(digest).cast(StringType))
        .withColumn("p_linenumberA", lit(p_linenumberA).cast(LongType))
        .withColumn("p_urltime", lit(p_urltime).cast(StringType))
        .withColumn("p_mime", lit(p_mime).cast(StringType))
        .withColumn("p_url", lit(p_url).cast(StringType))


      //   finalDFInner.rdd.fold("")((s1, s2) => s1 + ", " + s2)

      initialDF = initialDF.unionAll(finalDFInner)

    })

    //  println(" line number 253 , finalDF2")
    initialDF.show(20)

    // *****************************************************


    // udf = concat( p_linenumberA,  p_urltime,  p_mime, p_url )  ?
    // withColumn("new_col", udf())
    // or we can create new concat column in sql
    //initialDF.show(100, truncate = false)
    val fileOuptut = initialDF.map( x =>{

      var ln : Long = 0
      var line = ""
      var p_urltime = ""
      var p_mime = ""
      var p_url = ""
      var digest = ""


      //  try {

      ln = x.getAs[Long]("p_linenumberA")

      line = x.getAs[String]("line")
      p_urltime = x.getAs[String]("p_urltime")

      p_mime = x.getAs[String]("p_mime")
      p_url = x.getAs[String]("p_url")

      digest = x.getAs[String]("digest")
      /*}
      catch
        {
          case e:Exception => println("$$$$$$$$$$$$$$$$$$error in transform ")

        }
*/
      if(p_mime.contains("image/jpeg") || p_mime.contains("image/gif"))
      {
        var igLine:Boolean = false
        line.foreach(x =>{
          if(x.toInt > 128)
            igLine = true
        }
        )
        if(igLine)
          line = ""
        line = line.filter(_ >= ' ').replaceAll("\\u0001\\u0002\\u0003\\u0003\\u0005\\u0004\\u0005\\t\\u0006\\u0006", "")
        ((ln, p_urltime, p_mime, p_url, digest), line)
      }
      else {
        ((ln, p_urltime, p_mime, p_url, digest), line)
      }
    }).reduceByKey((iter, lineval) => {
      //Content-Length

      iter + "æ" + lineval
    })

    //println(" line number 284 , fileOuptut")


    // *****************************************************
    var finalData = new ListBuffer[finalOutput]()
    var title = ""
    var metadata = ""

    var contentLength = ""
    var etag = ""
    var lastmodified = ""
    var htmldate = ""
    //println("test 1")

    fileOuptut.toDF().printSchema()
    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    // println("test 3")

    fileOuptut.collect().foreach( x => {


      title = ""
      metadata = ""

      contentLength = ""
      etag = ""
      lastmodified = ""
      htmldate = ""

      val sub_lines = x._2.split("\n")

      sub_lines.foreach(s => {
        if (s.contains("Date:")) {
          htmldate = s.substring(s.indexOf("Date:") + 5)
          htmldate = htmldate.substring(0, htmldate.indexOf("æ")).trim()
        }
        if (s.contains("ETag:")) {
          etag = s.substring(s.indexOf("ETag:") + 6)
          etag = etag.substring(0, etag.indexOf("æ")).trim()
        }

        if (s.contains("Content-Length:")) {

          contentLength = s.substring(s.indexOf("Content-Length:") + "Content-Length:".length)
          contentLength = contentLength.substring(0, contentLength.indexOf("æ")).trim()

        }
        if (s.contains("Last-Modified:")) {
          lastmodified = s.substring(s.indexOf("Last-Modified:") + "Last-Modified:".length)
          lastmodified = lastmodified.substring(0, lastmodified.indexOf("æ")).trim()
        }

      })

      if(x._1._3.contains("text/html") ) {

        val doc1 = Jsoup.parse(x._2)
        val text = doc1.body.text.replaceAll("æ", "")
        title = if(!doc1.select("title").isEmpty ) doc1.select("title").first.text else ""
        metadata = if(!doc1.select("meta").isEmpty)  doc1.select("meta").first.attr("content") else ""
        //contentLength: String,etag:String,
        //htmldate:String,lastmodifieddate:String,
        var datetimeext = format.format(new java.util.Date())
        finalData += finalOutput(x._1._1.toString  ,x._1._2 , x._1._3 , x._1._4 , text, title, metadata
          ,contentLength, etag, htmldate, lastmodified, "", x._1._5, datetimeext, language)
      }
      else if(x._1._3.contains("text/xml"))
      {


        val inputstring = x._2.substring( x._2.indexOf("<?xml version="))

        var finalstring = ""
        try {
          val raw = xml.XML.loadString(inputstring)
          raw.child.foreach(x => {
            if (!x.text.trim.isEmpty) finalstring += x.text + ","
          })

        }catch{
          case e:Exception => finalstring = "Error Parsing - Original Text:" + x._2
        }

        finalstring = finalstring.replaceAll("æ", "")
        val datetimeext = format.format(new java.util.Date())
        finalData += finalOutput(x._1._1.toString  ,x._1._2 , x._1._3 , x._1._4 , finalstring, title, metadata
          ,contentLength, etag, htmldate, lastmodified, "", x._1._5, datetimeext, language)

      }
      else if(x._1._3.contains("text/css") || x._1._3.contains("text/dns"))
      {
        //println(x._1._2 + " - " + x._1._3 + " - " + x._1._4 + "--------" + x._2)// is it working?
        // connection problem - it doesnt work here
        //all paths are hdfs based.. u need to test this on VM
        // yes but cloudera is down at the moment..pls call me
        val data = x._2.replaceAll("æ", "")
        val datetimeext = format.format(new java.util.Date())
        finalData += finalOutput(x._1._1.toString  ,x._1._2 , x._1._3 , x._1._4 , data, title, metadata
          ,contentLength, etag, htmldate, lastmodified, "", x._1._5, datetimeext, language)
      }
      else if(x._1._3.contains("image/jpeg") || x._1._3.contains("image/gif") || x._1._3.contains("image/png")
        || x._1._3.contains("application/x-shockwave-flash") || x._1._3.contains("html/xml"))
      {
        // Exclude above mime types
        /*if(!x._2.isEmpty) {
          val data = x._2.replaceAll("æ", "")
          val datetimeext = format.format(new java.util.Date())
          finalData += finalOutput(x._1._1.toString, x._1._2, x._1._3, x._1._4, data, title, metadata
            ,contentLength, etag, htmldate, lastmodified, "", x._1._5, datetimeext, language)
        }*/
      }
      else
      {
        //println(x._2)
        val data = x._2.replaceAll("æ", "")
        val datetimeext = format.format(new java.util.Date())
        finalData += finalOutput(x._1._1.toString  ,x._1._2 , x._1._3 , x._1._4 , data, title, metadata
          ,contentLength, etag, htmldate, lastmodified, "", x._1._5, datetimeext, language)
      }
    }
    )




    // *****************************************************


    /*length,        -- contentLength
      language,      -- En    // en
      url,            -- p_url
      textTitle,      -- title
      detectedMime,    -- p _mime
    real_originator,    // ignore
    urlstore_id,       //  ignore
    timestamp,       --  p_urltime
      indexingTime,  --  currenttime stamp yyyyMMddHHmmss
      textContent,    -- line
      digest         -*/
    /*x._1._3.contains("image/jpeg") || x._1._3.contains("image/gif") || x._1._3.contains("image/png")
    || x._1._3.contains("application/x-shockwave-flash") || x._1._3.contains("html/xml")*/
    val fDf2 = JobSparkConf.sc.parallelize(finalData.toList).toDF("ln", "timestamp", "detectedMime", "url",
      "textContent","textTitle","metadata", "length", "etag", "htmldate", "lastmodifieddate", "imageData",
      "digest", "indexingTime","language")
    val fDf1 =   fDf2.filter(
      !fDf2("detectedMime").contains("image/jpeg")
        || !fDf2("detectedMime").contains("image/gif")
        || !fDf2("detectedMime").contains("image/gif")
        || !fDf2("detectedMime").contains("image/png")
        || !fDf2("detectedMime").contains("application/x-shockwave-flash")
        || !fDf2("detectedMime").contains("html/xml")
    )
      .select(
        "length",
        "language",
        "url",
        "textTitle",
        "detectedMime",
        "timestamp",
        "indexingTime",
        "textContent",
        "digest"
      )

    println("********** below should not retunr any records")
    fDf1.filter(fDf1("detectedMime").contains("image/gif")).show(100)
    /*
        def createUIDforResponse(df: DataFrame, sid: String) : DataFrame = {
          df.withColumn("createdUid", udf((str: String, surveyid: String)=>

            str + "_" + surveyid).apply(df("uuid"), lit(sid)))
        }*/

    /* val fDf = fDf1.withColumn("line",
       udf((title: String) =>
         if (title == null) title else {
           title
         } )
         .apply(fDf1("p_url")))
*/
    //   fDf1.write.json("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\esoutput10.json")
    fDf1.show(10)
    import util.Properties
    val sbJson = StringBuilder.newBuilder
    var indexcounter :Int = 0
    fDf1.toJSON.collect().foreach(row => {
      // println(row.mkString)
      sbJson.append( "{\"index\":{\"_index\":\"esload\",\"_type\":\"act1\",\"_id\":" + indexcounter.toString + "}}"  + Properties.lineSeparator)
      sbJson.append(row.mkString + Properties.lineSeparator)
      indexcounter = indexcounter + 1
      // row.toString
    })

    import java.io._
    val jsonoutputforElasticSearch = "C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\esoutput_for_es_" + format.format(new java.util.Date())  + ".json"
    val file = new File(jsonoutputforElasticSearch)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(sbJson.toString)
    bw.close()
    //  println(sbJson.toString)

    // going for run.. catch u later  ok
    /*
    import org.json4s.native.JsonMethods._
    import org.json4s.JsonDSL.WithDouble._

    val json = "detectedMime" -> fDf1.collect().toList.map{
      case (name, nodes) =>
        ("name", name)
          ("nodes", nodes.map{
            name => ("name", name)
          })
    }
*/
    // println(compact(render(json))) // Printing the rendered JSON*/



  //  fDf1.coalesce(1).write.json("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\esoutput16.json")
    /*

    * */



    // fDf1.coalesce(1).write.mode(SaveMode.Append).format("json").save("hdfs://139.162.54.153:8020/esoutput_280517_1730")

    //  EsSpark.saveToEs(fDf.rdd, "spark/docs" , Map("es.nodes" -> "192.168.0.56"))
    // fDf.coalesce(1).write.partitionBy("p_urltime","p_url").save("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput")
    // fDf.coalesce(1).write.partitionBy("p_urltime","p_url").save("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_parquet")

    // fDf.coalesce(1).save("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput2\\esoutputfile.csv", "com.databricks.spark.csv")


    // fDf.coalesce(1).write.partitionBy("p_urltime","p_url").save("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput")
    // fDf.coalesce(1).write.mode(SaveMode.Append).save("hdfs://139.162.54.153:8020/esoutput_270517_1618")
    //  fDf.coalesce(1).write.mode(SaveMode.Append).save("hdfs://192.168.56.102:8020/es/esoutput_270517_1618")
    //  fDf.write.mode(SaveMode.Append).save("hdfs://192.168.56.102:8020/es/esoutput_270517_1744")
    //D(Seq(fDf))

    // EsSpark.saveToEs(fDf.rdd, "spark/docs" , Map("es.nodes" -> "192.168.0.56"))


    // fDf.coalesce(1).write.mode(SaveMode.Append).save("hdfs://139.162.54.153:8020/esoutput_270517_1618")

    // fDf.coalesce(1).write.partitionBy("p_urltime","p_url").format("avro").save("hdfs://192.168.56.102:8020/esoutput")


    // 20090831084108 - image/jpeg - http://www.biglotteryfund.org.uk/index/newsroom-uk/openevent_08_2.jpg--------


    // finalDF2.count()
    // finalDF1.registerTempTable("cdxJoinArcTable")
    // finalDF1.printSchema()
    // finalDF1.show(100, truncate = false)


    //  fDf.coalesce(1).write.partitionBy("p_urltime","p_url").save("hdfs://139.162.54.153:8020/jatestfolder3")   // 192.168.56.102 139.162.54.153

    //   fDf.saveAsTextFile("hdfs://139.162.54.153:8020/jatestfolder")
    // fDf.coalesce(1).write.partitionBy("p_urltime","p_url").format("avro").save("hdfs://139.162.54.153:8020/esindex")

    // fDf.coalesce(1).write.partitionBy("p_urltime","p_url").format("com.databricks.spark.avro").save("hdfs://127.0.0.1:8020/esoutput")

    //  fDf.coalesce(1).write.partitionBy("p_urltime","p_url").format("avro").save("hdfs://192.168.56.102:8020/esindex")
    //  fDf.coalesce(1).write.partitionBy("p_urltime","p_url").format("avro").save("hdfs://192.168.56.102:8020/esindex")

    //fDf.coalesce(1).write.mode(SaveMode.Append).save("hdfs://127.0.0.1:8020/esoutput")
    //fDf.saveAsTextFile("hdfs://192.168.56.101:8020/query20")

    //  fDf.saveToEs(rdd, "spark/docs" , Map("es.nodes" -> "192.168.0.56")

    // .df.filter("doctor > 5").write.format("com.databricks.spark.avro").save("/tmp/output")

    // df.filter("doctor > 5").write.avro("/tmp/output")

    //fDf.p
    //fDf.patitionBy("").saveAsTextFile("hdfs://139.162.54.153:8020/jatestfolder1")
    //countByNumber_sortedDsc.saveAsTextFile("hdfs://139.162.54.153:8020/jatestfolder")
    // fDf.coalesce(1).write.mode(SaveMode.Append).save("hdfs://139.162.54.153:8020/jatestfolder")
    //.saveAsTextFile("hdfs://139.162.54.153:8020/query6_dir1_jagan1")
    //println(" line number 202 , key - values")
    // *****************************************************

    /*

    [html/xml]
    [unknown]
    [image/jpeg]
    [application/x-shockwave-flash] [html/xml]
    [image/gif]
    [image/png]


    +-----+--------------+----------+--------------------+--------------------+-----+--------+-------------+--------------------+--------------------+--------------------+---------+--------------------+
    |   ln|     p_urltime|    p_mime|               p_url|                line|title|metadata|contentLength|                etag|            htmldate|    lastmodifieddate|imageData|              digest|
    +-----+--------------+----------+--------------------+--------------------+-----+--------+-------------+--------------------+--------------------+--------------------+---------+--------------------+

    length,        -- contentLength
    language,      -- En    // en
    url,            -- p_url
    textTitle,      -- title
    detectedMime,    -- p _mime
                                  real_originator,    // ignore
                                  urlstore_id,       //  ignore
    timestamp,       --  p_urltime
    indexingTime,  --  currenttime stamp yyyyMMddHHmmss
    textContent,    -- line
    digest         -
    ln|     p_urltime|    p_mime|               p_url|                line|title|metadata|contentLength|                etag|            htmldate|    lastmodifieddate|imageData|              digest|
    */


  }

}

