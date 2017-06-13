package ja.conf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext}
import org.slf4j.LoggerFactory


/**
  * To configure the Spark job
  *
  */

trait Conf {
  val conf: SparkConf
  val sqlContext: SQLContext

}
object JobSparkConf extends Conf{
  val conf = new SparkConf()
    .setAppName("Spark ETL Job").setMaster("local[*]")
    .set("es.index.auto.create", "true")
   // .set("es.nodes.client.only", "true")
    .set("es.input.json", "true")
    //.set("es.nodes.client.only", "true")
    .set("es.nodes", "192.168.0.56")
    .set("es.port", "9200")   // elasticsearch.url: "http://192.168.0.56:9200/"
    .set("elasticsearch.url", "http://192.168.0.56:9200")

    .set("http.enabled", "true")
    .set("node.data", "true")
    .set("node.master", "true")
    .set("node.ingest", "true")



 // conf.set("es.index.auto.create", "false")
    // .set("es.cluster", "my-application")  // TODO: my-application pip6fRVRQkGRE4PQ9yqXKA
  //  .set("es.node", "192.168.0.56")
  //  .set("es.port", "9200")
   // .set("es.nodes.discovery", "false")

  /*.set("spark.sql.sources.default", "json") Agwal997Rhaw8Z7r6RluMA
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .set("spark.sql.tungsten.enabled", "true")
  .set("spark.driver.allowMultipleContexts", "true")*/

  val sc = new SparkContext(conf)

  sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", "AKIAIYPMPOB626HX6Y2A")
  sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "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")


//    [default]
//  aws_access_key_id = AKIAIYPMPOB626HX6Y2A
//  aws_secret_access_key = 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
//    [ja]
//  aws_access_key_id = AKIAI7H4MZPPUWN3Z6YA
//  aws_secret_access_key = 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
//    [profilename]
//  aws_access_key_id = AKIAJJT6AEXLSYAMS2SQ
//  aws_secret_access_key = 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
//
//  sc.hadoopConfiguration.set("fs.s3.aws_access_key_id", "AKIAIYPMPOB626HX6Y2A")
//  sc.hadoopConfiguration.set("fs.s3.aws_secret_access_key", "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")
//
  // Logging
  val sqlContext = new SQLContext(sc)

  System.setProperty("spark.ui.showConsoleProgress", "false")
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("elasticsearch").setLevel(Level.WARN)


  // Enable snappy compression for Avro
  //   hiveContext.setConf("spark.sql.avro.compression.codec", "snappy")

  val log = LoggerFactory.getLogger(this.getClass.getName)
}

