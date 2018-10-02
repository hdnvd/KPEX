package ir.sweetsoft.kpex

import java.io.BufferedOutputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class IO(ApplicationContext:KpexContext) extends KpexClass(ApplicationContext) {




  def WriteToFile(spark: SparkSession, Path: String, Content: String, Append: Boolean): Unit = {

    if (appContext.AppConfig.StorageType == appContext.AppConfig.FILE_MODE) {

      if (!Append && scala.tools.nsc.io.Path(Path).exists)
        scala.tools.nsc.io.Path(Path).delete()
      scala.tools.nsc.io.Path(Path).createFile().appendAll(Content)
    }
    else {
      if (Append)
        throw new Exception("This Part is Not Implemented Yet By Hadi Nahavandi")
      val thePath = new Path(Path)
      val fs = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
      val output = fs.create(thePath)
      val os = new BufferedOutputStream(output)
      os.write(Content.getBytes("UTF-8"))
      os.close()
      fs.close()
    }
  }
}
