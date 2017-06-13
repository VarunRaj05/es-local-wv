package ja.com

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory


object hdfsFileConfig
{
  val log = LoggerFactory.getLogger(this.getClass.getName)
  def getConfForHDFSFileSystem(config: Config): Configuration = {
    System.setProperty("HADOOP_USER_NAME", config.getString("app.directory.hdfsUser"))
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new Configuration()
    conf.set("fs.defaultFS", config.getString("app.directory.hdfsDefaultFs"))
   /* UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(config.getString("app.directory.hdfsUser") + "@" + config.getString("app.directory.hdfsUserDomain"),
      config.getString("app.directory.hdfsUserKeyTabFile"))
*/
    conf
  }

}