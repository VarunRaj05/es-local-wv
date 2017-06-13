package ja.com

import ja.conf.JobSparkConf

/**
  * Created by Ja on 01/06/2017.
  */
object awswc {
  def main(args: Array[String]): Unit = {

    //
    //

  //  var dnsfiles = "/whiltelist/dns_*"
   // var username = "edureka"

    // var username = "cloudera"
    //var username = "root"

  /*  val config: Config = ConfigFactory.load("application")
    val exclude_mimetypes = config.getStringList("app.directory.exclude_mimetypes").toList
    val exclude_fileext = config.getStringList("app.directory.exclude_fileext").toList
    val exclude_responsecodes = config.getStringList("app.directory.exclude_responsecodes").toList*/

    val language = "En"
    var cdxfile=   "s3://mw-tna-indexing/cdx/BL-100267.cdx"  //"s3://mw-tna-indexing/esoutput1/*" // "s3n://mw-tna-cdx/cdx/BL-100267.cdx" // "s3://mw-tna-cdx/cdx/BL-100267.cdx"
    var  arcfile  = "s3://mw-tna-webarchives/webarchives/BL-100267.arc.gz"
    var  outputpath  =  "s3://esoutput1/testfile.txt"

    val rdd = JobSparkConf.sc.textFile(outputpath)
    val counter = rdd.flatMap(x => x.split(" ")).map( x => (x, 1)).reduceByKey(_ + _)

    counter.take(1).foreach(println)


     counter.coalesce(1).saveAsTextFile(outputpath + "wc.txt")

  }
}
