package ja.conf

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

class JobConfigBase(configFilePath: String) {

    //val log = LoggerFactory.getLogger(this.getClass.getName)
    val config: Config = if(configFilePath.isEmpty) ConfigFactory.load("application") else ConfigFactory.parseFile(new File(configFilePath))
}

class JobConfig(configFilePath: String) extends JobConfigBase(configFilePath){


    val localcdxfilepath = config.getString("my.es.localcdxfilepath")
    val localarcfilepath = config.getString("my.es.localarcfilepath")
    val localSearchWhitelist = config.getString("my.es.localSearchWhitelist")

    val commonVariableResponseTable = config.getStringList("atomic.response.base_variables").toList

    def getConfigValue(key:String) : String = {
        var configValue = ""

        try {
            configValue = config.getString(key)
        }
        catch

            {case e: Exception =>
                println(s"Exception in getting config Value $key")
            }

        configValue
    }

}
