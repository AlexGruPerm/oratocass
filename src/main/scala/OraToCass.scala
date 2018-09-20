import java.io.File

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
/*
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession, SQLContext}

import org.apache.spark.sql.types._
import java.util.Properties
import com.datastax.spark.connector._
*/

object OraToCass extends App {

  val logger = LoggerFactory.getLogger(OraToCass.getClass)

  logger.info("BEGIN ..............")

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder().master("local")
    .appName("oratocass")
    .config("spark.driver.memory", "2G")
    .config("spark.executor.memory", "2G")
    //.config("spark.sql.warehouse.dir",warehouseLocation)
    //.config("hive.exec.dynamic.partition","true")
    //.config("hive.exec.dynamic.partition.mode","nonstrict")
    //.enableHiveSupport()
    .getOrCreate()

  val user_login       = "MSK_ARM_LEAD"
  val user_password    = "MSK_ARM_LEAD"
  val oracleUrl        = "10.127.24.11:1521/test"
  val url_string       = "jdbc:oracle:thin:"+user_login+"/"+user_password+"@//"+oracleUrl
  val parallelismLevel = 1

 // case class DDATE(ddate :Int)

  /*
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\ojdbc6.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\spark-cassandra-connector-assembly-2.3.2-11-gdbe6c052.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\jackson-annotations-2.9.6.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\jackson-core-2.9.6.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\jackson-databind-2.9.6.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\jackson-module-scala_2.12-2.9.6.jar")
  */

  val ddateList = spark.read
  .format("jdbc")
    .option("url", url_string)
    .option("dbtable", s"(select distinct DDATE from t_data)")
    .option("user", user_login)
    .option("password", user_password)
    .option("customSchema","DDATE INT")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()
   // .collect.toSeq
    //.as[DDATE].collect.toSeq

  ddateList.printSchema()

  ddateList.show(10)

/*
    .option("fetchSize", "100")
    .option("partitionColumn", "DDATE")
    .option("lowerBound", 20160601)
    .option("upperBound", 20180601)
    .option("numPartitions", parallelismLevel)
*/

  //ddateList.printSchema()

  //ddateList.show(10)

  /*
  for (ddate <- ddateList) {
    println("ddate=" + ddate)
  }
*/

}
