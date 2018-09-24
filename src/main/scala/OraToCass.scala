import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/*
import org.apache.spark.sql.Encoders

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

  logger.info("BEGIN [OraToCass]")

  //val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder().master(/*"spark://172.18.16.71:7077"*/"local")
    .appName("oratocass")
    //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.driver.memory", "4G")
    .config("spark.executor.memory", "1G")
    //.config("spark.sql.warehouse.dir",warehouseLocation)
    //.config("hive.exec.dynamic.partition","true")
    //.config("hive.exec.dynamic.partition.mode","nonstrict")
    //.config("hive.server2.idle.operation.timeout","10000ms")
    //.config("hive.server2.idle.session.timeout","10000ms")
    //.enableHiveSupport()
    .config("spark.cassandra.connection.host", "127.0.0.1")
   // .config("spark.driver.allowMultipleContexts","true")
    .config("spark.cassandra.input.split.size_in_mb","128")
    .config("spark.cassandra.input.fetch.size_in_rows","10000")
    .getOrCreate()

  val user_login       = "MSK_ARM_LEAD"
  val user_password    = "MSK_ARM_LEAD"
  val oracleUrl        = "10.127.24.11:1521/test"
  val url_string       = "jdbc:oracle:thin:"+user_login+"/"+user_password+"@//"+oracleUrl
  val parallelismLevel = 1

  case class DDATE(ddate :Int)

  case class POK(id_pok :Int)

  case class T_DATA_ROW(ddate: Int, id_pok :Int, id_row :String,sval :String)

  /*
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\ojdbc6.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\spark-cassandra-connector-assembly-2.3.2-11-gdbe6c052.jar")
  */

  import spark.implicits._

def getDistinctDDates() = {
  val ddateList = spark.read.format("jdbc")
    .option("url", url_string)
    //.option("http.header.Connection","close")
    .option("dbtable", "Javachain_Oracle.Javachain_log")
    .option("user", "MSK_ARM_LEAD")
    .option("password", "MSK_ARM_LEAD")
    .option("dbtable", s"(select distinct DDATE from t_data)")
    //.option("fetchSize", "1000")
    .option("customSchema", "DDATE INT")
    .load().as[DDATE].cache()
  ddateList
}

  val ddateList = getDistinctDDates()

  ddateList.printSchema()

  logger.info("ddateList.getClass.getName="+ddateList.getClass.getName)
  logger.info("ddateList.getClass.getTypeName="+ddateList.getClass.getTypeName)
  logger.info("ddateList.isLocal="+ddateList.isLocal)

  ddateList.persist()

  def getPoksByDDate(inDDate :Int) = {

    val poksSql = s"(select distinct ID_POK from t_data where ddate="+inDDate+" order by 1)"


    val isPoksList = spark.read
      .format("jdbc")
      .option("url", url_string)
      .option("dbtable", poksSql)
      .option("user", user_login)
      .option("password", user_password)
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("customSchema","ID_POK INT")
    .load().as[POK].cache()

    /*
    val isPoksList = spark.read.format("jdbc")
      .option("url",url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user",user_login)
      .option("password", user_password)
      .option("dbtable",poksSql)
      .option("customSchema","ID_POK INT")
      //.option("fetchSize", "1000")
      .option("numPartitions", "1")
      .load().as[POK].cache()
    */

    isPoksList
  }


  /*
  def getTDataByDDateIDPok(inDDate :Int,inIDPok :Int) = {

    val dataSql = s"(select DDATE,ID_POK,ID_ROW,val as SVAL from t_data where ddate={0} and id_pok={1} order by 1,2,3)".format(inDDate,inIDPok)

    val t_data_ds = spark.read.format("jdbc")
      .option("url",url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user",user_login).option("password", user_password)
      .option("dbtable",dataSql)
      // .option("dbtable",s"(select DDATE,ID_POK,ID_ROW,val as SVAL from t_data where ddate="+inDDate+" and id_pok="+inIDPok+" order by 1,2,3)")
      .option("customSchema","ddate INT,id_pok INT, id_row String,sval String")
      .option("fetchSize", "10000")
      .load().as[T_DATA_ROW]
    t_data_ds
  }
  */


  logger.info(" ====================================================================== ")
  //ddateList filter(d => d.ddate == 20180601) map(d => d.ddate) foreach {
  ddateList map(d => d.ddate) filter(_ == 20180601) foreach {
    thisDdate =>
      logger.info(" thisDdate = " + thisDdate)

      val poksList = getPoksByDDate(thisDdate)
      logger.info(">>>>> begin poks count >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
      logger.info("  poksList.count()=" + poksList.count())
      logger.info("<<<<< end poks count <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

/*
      poksList filter(p => p.id_pok == 2200) map(p => p.id_pok) foreach {
        thisPok =>
          val t_data_ds = getTDataByDDateIDPok(thisDdate, thisPok)
          logger.info("  thisPok=" + thisPok + " t_data_ds.count()=" + t_data_ds.count())

      }
      */
  }
  logger.info(" ====================================================================== ")


  //ddateList.show(10)

/*
    .option("fetchSize", "100")
    .option("partitionColumn", "DDATE")
    .option("lowerBound", 20160601)
    .option("upperBound", 20180601)
    .option("numPartitions", parallelismLevel)
*/



  ddateList.unpersist()

  logger.info("END [OraToCass]")
}
