import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.datastax.spark.connector._

object OraToCass extends App {

  val logger = LoggerFactory.getLogger(OraToCass.getClass)

  logger.info("BEGIN [OraToCass]")

  //val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder()
    .master(/*"spark://172.18.16.9:7077"*/"local[*]")
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
    //.config("spark.cassandra.input.split.size_in_mb","128")
    //.config("spark.cassandra.input.fetch.size_in_rows","10000")
    .config("spark.sql.shuffle.partitions","10")
    .getOrCreate()

  val user_login       = "MSK_ARM_LEAD"
  val user_password    = "MSK_ARM_LEAD"
  val oracleUrl        = "10.127.24.11:1521/test"
  val url_string       = "jdbc:oracle:thin:"+user_login+"/"+user_password+"@//"+oracleUrl
  //jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)
  val parallelismLevel = 1

  //case class DDATE(ddate :Int)

  //case class POK(id_pok :Int)

  case class DDATE_POK(ddate :Int,id_pok :Int)

  case class T_DATA_ROW(ddate: Int, id_pok :Int, id_row :String, sval :String)

  /*
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\ojdbc6.jar")
  spark.sparkContext.addJar("C:\\oratocass\\project\\lib\\spark-cassandra-connector-assembly-2.3.2-11-gdbe6c052.jar")
  */

  import spark.implicits._

def getDistinctDDatesIDPoks() = {
val ds = spark.read.format("jdbc")
  .option("url", url_string)
  .option("dbtable", "Javachain_Oracle.Javachain_log")
  .option("user", "MSK_ARM_LEAD")
  .option("password", "MSK_ARM_LEAD")
  .option("dbtable", s"(select distinct DDATE,ID_POK from t_data order by 1,2)")
  .option("numPartitions", "1")
  .option("customSchema", "DDATE INT,ID_POK INT")
  .load().as[DDATE_POK].cache()

  ds.printSchema()
  logger.info("ds.getClass.getName="+ds.getClass.getName)
  logger.info("ds.getClass.getTypeName="+ds.getClass.getTypeName)
  logger.info("ds.isLocal="+ds.isLocal)

ds
}



def getTDataByDDateIDPok(inDDate :Int, inIDPok :Int) = {

/*
import java.util.Properties
val df4parts = spark.
  read.
  jdbc(
    url = "jdbc:postgresql:sparkdb",
    table = "projects",
    predicates = Array("id=1", "id=2", "id=3", "id=4"),
    connectionProperties = new Properties())
*/

  val dataSql = s"(select DDATE,ID_POK,ID_ROW,VAL as SVAL from T_DATA subpartition (PART_"+inDDate+"_POK_"+inIDPok+") order by 1,2,3)"

  val t_data_ds = spark.read.format("jdbc")
    .option("url",url_string)
    .option("dbtable", "Javachain_Oracle.Javachain_log")
    .option("user",user_login).option("password", user_password)
    .option("dbtable",dataSql)
    .option("numPartitions", "1")
    .option("customSchema","DDATE INT, ID_POK INT, ID_ROW String, SVAL String")
    .option("fetchSize", "10000")
    .load()

  t_data_ds.explain()

  t_data_ds.as[T_DATA_ROW].cache()
}


  logger.info(" ====================================================================== ")
  val t1_common = System.currentTimeMillis

  val dsDdatesPoks = getDistinctDDatesIDPoks()
  logger.info("--------------------  dsDdatesPoks.count()="+dsDdatesPoks.count())

  //dsDdatesPoks filter(r => r.ddate == 20180601 && Seq(168,502,2000,2100).contains(r.id_pok)) explain()

  dsDdatesPoks filter(r => r.ddate == 20180601 && Seq(/*168,502,2000,*/2100).contains(r.id_pok)) foreach {
    thisRow =>

      val t1 = System.currentTimeMillis
      val t_data_ds = getTDataByDDateIDPok(thisRow.ddate, thisRow.id_pok)


      //logger.info(" t_data_ds.count()=" + t_data_ds.count())


      val collect = spark.sparkContext.parallelize(t_data_ds.collect.toSeq)

      collect.saveToCassandra("msk_arm_lead", "t_data")

      val t2 = System.currentTimeMillis

      logger.info("----------------------------------------------------------")
      logger.info("                                                          ")
      logger.info(" >   DDATE: " + thisRow.ddate + " ID_POK:" + thisRow.id_pok)
      logger.info(" >   Dur.  = "+(t2 - t1) + " ms.                          ")
      logger.info("                                                          ")
      logger.info("----------------------------------------------------------")
  }

  val t2_common = System.currentTimeMillis

  logger.info("----------------------------------------------------------")
  logger.info("                                                          ")
  logger.info(" >  COMMON DURATION = "+(t2_common - t1_common) + " ms.   ")
  logger.info("                                                          ")
  logger.info("----------------------------------------------------------")

 logger.info(" ================== END [OraToCass] ================================")
}
