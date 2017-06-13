package ja.com

import org.apache.spark.sql.functions.udf

/**
  * Created by Ja on 21/05/2017.
  */
object Common {
   case class elasticSearchDetails(timstamp: String,
      content_Type:String,URL : String, date : String, Text : String)
   case class cdxItem(massaged_url:String,
                        b_date:String,
                        a_origina_url :String,
                        m_mime_type_of_original_document:String,
                        s_response_code : String,
                        digest: String,
                        r_redirect :String                      ,
                        code_abrevation : String,
                        V_compressed_arc_file_offset : String,
                        file_name:String

   )


      def toFindlineNum(s: String): String = {
         //  val  s = "(http://www.biglotteryfund.org.uk/robots.txt 77.108.154.94 20090831083146 unknown 223,22)"
         val Lastval = s.substring(s.toString().lastIndexOf(',') +1) .replace(")","")
         Lastval
        //   println(Lastval)
      }

   def toGetURL(s: String): String = {
      var retvalue = ""
      if (s != null && s != "" ) {
         retvalue = s.replaceAll("http://","").replaceAll("https://","").replaceAll("dns:","")
         if(retvalue.toString.indexOf("/") >= 0)
            retvalue = retvalue.substring(0,retvalue.indexOf("/"))
         if(retvalue.toString.indexOf(":") >= 0)
            retvalue = retvalue.substring(0,retvalue.indexOf(":"))
      }
      retvalue
   }

   val toGetURLnew = udf[String, String] {
      toGetURL
   }

}
