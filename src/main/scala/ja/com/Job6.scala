package ja.com

import java.io._
import java.nio.charset.Charset
import java.util.regex.Matcher
import ja.com.Common._
import com.typesafe.config.{Config, ConfigFactory}
import ja.Tikamanager.jatikaparser
import ja.com.Common.cdxItem
import ja.conf.JobSparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.archive.io.ArchiveRecord

import scala.collection.JavaConversions._

object Job6 {

  case class t(x : Long, y: String)
  case class allLines( linenumber: Long  , isvalidheader:Boolean ,isheader:Boolean, url:String, urltime:String, mime:String , line:String)

  case class finalOutputNew(line: Int, timestamp:String, detectedMime:String, url:String,
                            textTitle:String, length: String,
                         arcBody : Array[Byte], textContent:String,  digest: String, indexingTime: String, language: String)
  import java.util.regex.Pattern
  val dataLinePattern = Pattern.compile("(\\d{14})")

  def getDomainsDf(dnsfiles : String) : DataFrame = {


    import JobSparkConf.sqlContext.implicits._
    val inscopeDomains = JobSparkConf.sc.textFile(dnsfiles)   //



    val domainsDf =   inscopeDomains.map( x => {
      if(x.indexOf("/") >= 0 )
        x.substring(0,x.indexOf("/")).replaceAll("\"", "")
      else
        x.replaceAll("\"", "")
    }).toDF("domain")
    println("domainsDf.Count=" + domainsDf.count())
    domainsDf
  }
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

  def readHdfsFile( filePath: String): InputStreamReader = {
    val config: Config = ConfigFactory.load("application")
    val hdfsConf = hdfsFileConfig.getConfForHDFSFileSystem(config)
    val fs = FileSystem.get(hdfsConf)
    import java.io.InputStreamReader
    val pt = new Path(filePath)
    //if (fs.exists(pt)) {
    val stream =  new InputStreamReader(fs.open(pt))
    stream
  }

  def excludebyExt(exclude_fileext : List[String], compareText : String) : Boolean = {
    var ret = false
    for(item <- exclude_fileext){
        if(compareText.contains(item) )
          ret = true
    }
    ret

  }
  def main(args: Array[String]): Unit = {       // 2

//    val language = "En"
//    var arcfile = "/srcfileloc/*.arc"
//    var cdxfile = "/srcfileloc/*.cdx"
//    var dnsfiles = "/whiltelist/dns_*"
//    var username = "edureka"

//    s3://mw-tna-webarchives/webarchives/
//      s3://mw-tna-cdx/

//    https://s3-eu-west-1.amazonaws.com/mw-tna-cdx/cdx2/BL-100267.cdx
//    https://s3-eu-west-1.amazonaws.com/mw-tna-webarchives/webarchives/BL-100267.arc.gzhttps://s3-eu-west-1.amazonaws.com/mw-tna-webarchives/webarchives/BL-100267.arc.gz

    val language = "En"
    var arcfile = "s3://mw-tna-webarchives/webarchives/*.arc"
    var cdxfile = "s3://mw-tna-cdx/*.cdx"
    var dnsfiles = "/whiltelist/dns_*"
    var username = "edureka"

    // var username = "cloudera"
    //var username = "root"

    val config: Config = ConfigFactory.load("application")
    val exclude_mimetypes = config.getStringList("app.directory.exclude_mimetypes").toList
    val exclude_fileext = config.getStringList("app.directory.exclude_fileext").toList
    val exclude_responsecodes = config.getStringList("app.directory.exclude_responsecodes").toList
    //
    //    C:\Users\Ja\Google Drive\srcfile\Note\srcfile\latestarcfiles\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000
    //    C:\Users\Ja\Google Drive\srcfile\Note\srcfile\latestarcfiles\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc
    //    arcfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\Note\\srcfile\\EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.arc"
    //    cdxfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\srcfileloc\\*.cdx"
    //
    var filelocation : String = ""
    if(args.length > 0) {
      filelocation = args(0)
      if (filelocation.equals("local")) {
        // as we reading through file object give correct name in the arcfile below with .gz etn  ok

        // C:\Users\Ja\Google Drive\oldfiles\Note\srcfile\EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.cdx
        // C:\Users\Ja\Google Drive\oldfiles\Note\srcfile\EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.arc.gz
        // C:\Users\Ja\Google Drive\oldfiles\Note\srcfile\SearchWhitelist

  /*      arcfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\Note\\srcfile\\latestarcfiles\\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc\\*.arc"
        cdxfile = "C:\\Users\\Ja\\Google Drive\\srcfile\\Note\\srcfile\\latestarcfiles\\*.cdx"  // we dont need to do anything except that joining side - ok
        dnsfiles = "C:\\Users\\Ja\\Google Drive\\oldfiles\\Note\\srcfile\\SearchWhitelist\\dns_*"*/
                                                     // EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.arc.gz
                                                     // file:///{yourfilepath}
/*          arcfile = "C:\\Users\\Ja\\Downloads\\Archive\\EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.arc.gz" //give  thek full ofile  name
           cdxfile = "C:\\Users\\Ja\\Downloads\\EA-TNA-0709-biglotteryfund.org.uk-p-20090831083143-00000.cdx"
           dnsfiles = "C:\\Users\\Ja\\Google Drive\\oldfiles\\Note\\srcfile\\SearchWhitelist\\dns_*"*/

/*        arcfile = "C:\\Users\\Ja\\Downloads\\test_folder_for_pdf\\EA-TNA-0309-3719-0-20090518081242-00000.arc.gz" //give  thek full ofile  name
        cdxfile = "C:\\Users\\Ja\\Downloads\\test_folder_for_pdf\\EA-TNA-0309-3719-0-20090518081242-00000.cdx"
        dnsfiles = "C:\\Users\\Ja\\Google Drive\\oldfiles\\Note\\srcfile\\SearchWhitelist\\dns_*"*/




        arcfile = "C:\\Users\\Ja\\Downloads\\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.arc.gz" //give  thek full ofile  name
        cdxfile = "C:\\Users\\Ja\\Downloads\\IM_TNA_monthly_042016_www.open-water.org.uk-20160415110019-00000.cdx"
        dnsfiles = "C:\\Users\\Ja\\Google Drive\\oldfiles\\Note\\srcfile\\SearchWhitelist\\dns_*"


        println("$$$$$$$ arcfile " +arcfile)
        println("$$$$$$$ cdxfile " +cdxfile)
        println("$$$$$$$ dnsfiles " +dnsfiles)

      }
    }

    println(arcfile)
    println(cdxfile)
    println(dnsfiles)
    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    val output = new FileOutputStream("application.conf")
    System.setProperty("HADOOP_USER_NAME", username)

    // ********************************************

    val txtRDD = JobSparkConf.sc.textFile(cdxfile)
    //val txtRDD = JobSparkConf.sc.textFile("/srcfileloc/*.CDX")
    val rddLines = txtRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    import JobSparkConf.sqlContext.implicits._
    val cdxItems1 = rddLines.map(x => {
        if (exclude_mimetypes.contains(x.split(" ")(3).trim) || excludebyExt(exclude_fileext, x.split(" ")(2).trim)
          || exclude_responsecodes.contains(x.split(" ")(4).trim)) {
          cdxItem("", "", "", "", "", "", "", "", "", "")
        }
        else {
          cdxItem(x.split(" ")(0), x.split(" ")(1), x.split(" ")(2), x.split(" ")(3),
            x.split(" ")(4), x.split(" ")(5), x.split(" ")(6), x.split(" ")(7), x.split(" ")(9), x.split(" ")(10))
        }
      }
    ).toDF()


    val filteredCDXitems1 = cdxItems1.filter(!cdxItems1("massaged_url").equalTo("")).toDF()

    val filteredCDXitems = filteredCDXitems1.withColumn("New_URL", toGetURLnew(filteredCDXitems1("a_origina_url")))

    filteredCDXitems.select("m_mime_type_of_original_document").distinct().show(100, truncate = false)
    filteredCDXitems.select("m_mime_type_of_original_document").distinct().rdd.coalesce(1).saveAsTextFile("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\distinct_mimetype_output_"+ format.format(new java.util.Date() ))
    filteredCDXitems.show(10)


    filteredCDXitems.registerTempTable("mainT")

    val domainsDf =  getDomainsDf(dnsfiles)

    domainsDf.registerTempTable("domainT")

    val joinedDF = JobSparkConf.sqlContext.sql("select mainT.* from mainT inner join domainT" +
      " ON mainT.New_URL LIKE concat('%',domainT.domain,'%') ")

    joinedDF.registerTempTable("joinedTable")

    joinedDF.show(10)
    // 20090831084108 - image/jpeg - http://www.biglotteryfund.org.uk/index/newsroom-uk/openevent_08_2.jpg-
    /// Arc file starts here

    println("joinedDF.Count =" + joinedDF.count())



    println(" ***************************************************** End of CDX parsing ")
//Todo : for hdfs file
 /*   val file = new File(arcfile)
    import org.archive.io.arc._

    val arcreader = ARCReaderFactory.get(file)
    val arcrecord = arcreader.get(215743)*/

    //val fstream = readHdfsFile(arcfile) // if(!filelocation.equals("local"))  else None

   // val file = new File(arcfile)                                 //  val file1 = new File(arcfile)   thinking about hadoop files..this will work for local .. not for hadoop

    import org.archive.io.arc._
    //val arcReader = ARCReaderFactory.get(arcfile)           // val arcReader = ARCReaderFactory.get(file1)

    //println(arcrecord.getHeader.getUrl)

    var line = 1
    val lstFinalDataToSave = joinedDF.collect().map(cdxItem => {
      val V_compressed_arc_file_offset = cdxItem.getAs[String]("V_compressed_arc_file_offset").trim
        //  println(" V_compressed_arc_file_offset =" + V_compressed_arc_file_offset)    //
      val digest = cdxItem.getAs[String]("digest")
      val arcReader = ARCReaderFactory.get(arcfile)

      //println("********************************")
      //println("V_compressed_arc_file_offset =" + V_compressed_arc_file_offset)

      val arcrecord = arcReader.get(V_compressed_arc_file_offset.toLong)
      val url = arcrecord.getHeader.getUrl
      val mime = arcrecord.getHeader.getMimetype
      val arcTime = arcrecord.getHeader.getDate
      val contentLength = arcrecord.getHeader.getContentLength

      line = line + 1
      var bodyBytes = Array[Byte]()
      var imageData = ""
      val arctitle = "" // get from content body
      val indexingtime = format.format(new java.util.Date())
      try {
         if(url.equals("http://www.open-water.org.uk/media/2150/rmomg-summary-note-v2-november-meeting.doc"))
           {
             println("break")
           }
         bodyBytes = getArcRecordBody(arcrecord)
         imageData = new String(bodyBytes, Charset.forName("UTF-8"))

         if(mime.contains("text/html") || mime.contains("text/xml"))
           {
             imageData =  jatikaparser.parseTextFile(bodyBytes).filter(_ >= ' ').replaceAll("\\s{2,}", " ").trim
           }
         else if(mime.contains("pdf"))
           {
             imageData =  jatikaparser.parsePdfFile(bodyBytes).replaceAll("\\s{2,}", " ").trim
           }
         else if(mime.equals("application/msword"))
           {
             val xtype = url.endsWith("x")
            // println("********************************************************************")
            // println("****************")
            // println("url =" + url + " ------------  xtype=" + xtype)
            // println("original Body of message : " + imageData)
             //println("********************************************************************")
             imageData =  jatikaparser.parseDocFile(bodyBytes, xtype).replaceAll("\\s{2,}", " ").trim
             println(imageData)
            // println("********************************************************************")
           }
      }
      catch
        {
          case e:Exception => println("Exception while dumping " + e.getMessage)
        }
      finalOutputNew(line, arcTime , mime, url, arctitle, contentLength.toString, bodyBytes,imageData, digest, indexingtime, language)
    }).toList
//1 sec sir .. complete the final step
   /* line: Int, arcTime:String, mimeType:String, url:String,
    title:String, contentLength: String,
    arcBody : Array[Byte], arcBodyContent:String,  digest: String, indexingTime: String, language: String*/

    val rdd = JobSparkConf.sc.parallelize(lstFinalDataToSave, 1)
   val fDf1 = rdd.toDF().select("length","language","url","textTitle","detectedMime",
     "timestamp",     "indexingTime",     "textContent",     "digest")


    val fDf_demo1 = fDf1.select("length","language","url","textTitle","detectedMime",
      "timestamp",     "indexingTime",     "textContent",     "digest").filter(fDf1("detectedMime").contains("pdf")).limit(1)
    fDf_demo1.rdd.coalesce(1).saveAsTextFile("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\pdf_pdf_output_"  + format.format(new java.util.Date()))

    val fDf_demo2 = fDf1.select("length","language","url","textTitle","detectedMime",
      "timestamp",     "indexingTime",     "textContent",     "digest").filter(fDf1("detectedMime").contains("doc")).limit(1)
    fDf_demo2.rdd.coalesce(1).saveAsTextFile("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\doc_doc_output_"  + format.format(new java.util.Date()))

    val fDf_demo3 = fDf1.select("length","language","url","textTitle","detectedMime",
      "timestamp",     "indexingTime",     "textContent",     "digest").filter(fDf1("detectedMime").contains("html")).limit(1)
    fDf_demo3.rdd.coalesce(1).saveAsTextFile("C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\doc_html_output_"  + format.format(new java.util.Date()))

    fDf1.show(10 , truncate = true)
    val sbJson = StringBuilder.newBuilder
    var indexcounter :Int = 0
    fDf1.toJSON.collect().foreach(row => {

      sbJson.append( "{\"index\":{\"_index\":\"esload\",\"_type\":\"act1\",\"_id\":" + indexcounter.toString + "}}"  + util.Properties.lineSeparator)
      sbJson.append(row.mkString + util.Properties.lineSeparator)

      indexcounter = indexcounter + 1

    })

    import java.io._
    val jsonoutputforElasticSearch = "C:\\Users\\Ja\\Google Drive\\srcfile\\esoutput_json\\esoutput_for_es_" + format.format(new java.util.Date())  + ".json"
    val file2 = new File(jsonoutputforElasticSearch)
    val bw = new BufferedWriter(new FileWriter(file2))
    bw.write(sbJson.toString)
    bw.close()
    println("end")

  }

  def getArcRecordBody(arcrecord: ArchiveRecord) : Array[Byte] = {
    val os : ByteArrayOutputStream = new ByteArrayOutputStream()
    arcrecord.dump(os)
    val outputString = os.toString
    val bytes = os.toByteArray
    val original = new String(bytes, Charset.forName("UTF-8"))
    val lines = original.split("\r\n")
    var messagelength = 0
    //var message = ""
    var line = "test"
    var counter = 0
    // while loop execution
    while(!line.trim.isEmpty){
      line = lines(counter)
    //  message += line
      messagelength += (line.length + 2)
    //  println(messagelength)
    //  println(line)
      counter = counter + 1
    }

  //  val messageops = new scala.collection.immutable.StringOps(message)
  //  println("Bytes length :" + messageops.getBytes().length + " message length:" + messagelength)

    val newBytes = java.util.Arrays.copyOfRange(bytes,  messagelength , os.toByteArray.length)
   // println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^bytes")
    // println(original)
    //println("original bytes size" + bytes.length)


    //println(aftercut)

    newBytes

  }

}

