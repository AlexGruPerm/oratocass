import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
//import com.datastax.spark.connector._
import com.datastax.spark.connector._

object OraToCass extends App {

  val logger = LoggerFactory.getLogger(OraToCass.getClass)

  logger.info("BEGIN [OraToCass]")

  //val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("oratocass")
    .config("spark.driver.memory", "4G")
    .config("spark.executor.memory", "1G")
    .config("spark.cassandra.connection.host", "10.241.5.234")
    // .config("spark.sql.shuffle.partitions","10")
    // .config("spark.cassandra.input.split.size_in_mb","32")//256
    // .config("output.batch.size.bytes","8192")
    // .config("output.batch.size.bytes","512")
    .getOrCreate()


  // 127.0.0.1

  /*"spark://172.18.16.9:7077"*/
  //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //.config("spark.sql.warehouse.dir",warehouseLocation)
  //.config("hive.exec.dynamic.partition","true")
  //.config("hive.exec.dynamic.partition.mode","nonstrict")
  //.config("hive.server2.idle.operation.timeout","10000ms")
  //.config("hive.server2.idle.session.timeout","10000ms")
  //.enableHiveSupport()
  // .config("spark.driver.allowMultipleContexts","true")
  //.config("spark.cassandra.input.split.size_in_mb","128")
  //.config("spark.cassandra.input.fetch.size_in_rows","10000")

  val user_login       = "MSK_ARM_LEAD"
  val user_password    = "MSK_ARM_LEAD"
  val oracleUrl        = "10.127.24.11:1521/test"
  val url_string       = "jdbc:oracle:thin:"+user_login+"/"+user_password+"@//"+oracleUrl
  //jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)
  val parallelismLevel = 1

  //case class DDATE(ddate :Int)

  //case class POK(id_pok :Int)

  case class DDATE_POK(DDATE :Int,ID_POK :Int)

  case class DDATE_IDOIV(DDATE :Int,ID_OIV :Long)

  case class T_DATA_ROW(DDATE: Int, ID_POK :Int, ID_ROW :String, SVAL :String)

  case class T_DATA_STATS(TABLE_NAME :String, DDATE: Int, ID_POK :Int, ID_OIV :Long, ROW_COUNT :Long, INSERT_DUR_MS :Double)

  case class T_KEYS_ROW(
                         DDATE             :Int,
                         ddate_actual      :Int,
                         id_row            :String,
                         ID_OIV            :Long,
                         id_org            :Long,
                         id_class_grbs     :Int,
                         id_gp             :Int,
                         id_budget         :Int,
                         id_industry       :Int,
                         id_territory      :Int,
                         id_uk             :Int,
                         id_serv_work      :String,
                         id_msp            :Int,
                         id_pref           :Int,
                         id_income_budget  :Int,
                         id_gu_type        :Int
                       )

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
  //logger.info("ds.getClass.getName="+ds.getClass.getName)
  //logger.info("ds.getClass.getTypeName="+ds.getClass.getTypeName)
  //logger.info("ds.isLocal="+ds.isLocal)

ds
}


  def getDistinctDDatesIDOiv() = {
    val ds = spark.read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", "MSK_ARM_LEAD")
      .option("password", "MSK_ARM_LEAD")
      .option("dbtable", s"(select distinct ddate,id_oiv from T_KEYS order by 1,2)")
      .option("numPartitions", "1")
      .option("customSchema", "DDATE INT,ID_OIV BIGINT")
      .load().as[DDATE_IDOIV].cache()

    ds.printSchema()

    //logger.info("ds.getClass.getName="+ds.getClass.getName)
    //logger.info("ds.getClass.getTypeName="+ds.getClass.getTypeName)
    //logger.info("ds.isLocal="+ds.isLocal)

    ds
  }



def getTDataByDDateIDPok(inDDate :Int, inIDPok :Long) = {

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

  val dataSql = s"(select ddate,id_pok,id_row,val as sval from T_DATA subpartition (PART_"+inDDate+"_POK_"+inIDPok+") order by 1,2,3)"

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


  def getTKeysData(inDDate :Int,inIdOiv :Long)={

    val dataSql = s"(select * from T_KEYS PARTITION(PART_"+inDDate+") tk where tk.id_oiv="+inIdOiv+" order by ddate,id_oiv,id_row)"

    val t_data_ds = spark.read.format("jdbc")
      .option("url",url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user",user_login).option("password", user_password)
      .option("dbtable",dataSql)
      .option("numPartitions", "1")
      .option("customSchema"," ddate INT,ddate_actual INT,id_row String,id_oiv BIGINT,id_org BIGINT,id_class_grbs INT,id_gp INT,id_budget INT,id_industry INT,id_territory INT,id_uk INT,id_serv_work String,id_msp INT,id_pref INT,id_income_budget INT,id_gu_type INT")
      .option("fetchSize", "10000")
      .load()

    //t_data_ds.explain()

    t_data_ds.as[T_KEYS_ROW].cache()
  }


  logger.info(" ====================================================================== ")


  val t1_common = System.currentTimeMillis

  val dsDdatesPoks = getDistinctDDatesIDPoks()
  logger.info("--------------------  dsDdatesPoks.count()="+dsDdatesPoks.count())

  //dsDdatesPoks filter(r => r.ddate == 20180601 && Seq(168,502,2000,2100).contains(r.id_pok)) explain()

  dsDdatesPoks filter(r => r.DDATE == 20180601 && Seq(168,502,2000,2100).contains(r.ID_POK)) foreach {
    thisRow =>

      logger.info(" > thisRow.id_pok:" + thisRow.ID_POK)

      val t1 = System.currentTimeMillis
      val t_data_ds = getTDataByDDateIDPok(thisRow.DDATE, thisRow.ID_POK)


      val collect = spark.sparkContext.parallelize(t_data_ds.collect.toSeq)
      collect.saveToCassandra("msk_arm_lead", "t_data")

      val t2 = System.currentTimeMillis

      val rCount = t_data_ds.count()

      logger.info("----------------------------------------------------------")
      logger.info("                                                          ")
      logger.info(" >   DDATE:  " + thisRow.DDATE + " ID_POK:" + thisRow.ID_POK)
      logger.info(" >   ROWCNT: " + rCount)
      logger.info(" >   Dur.  = "+(t2 - t1) + " ms.                          ")
      logger.info("                                                          ")
      logger.info("----------------------------------------------------------")


      val dsStats = Seq(new T_DATA_STATS("T_DATA", thisRow.DDATE, thisRow.ID_POK, 0.toLong , rCount, (t2 - t1)))
        .toDF("table_name", "ddate", "id_pok", "id_oiv", "row_count", "insert_dur_ms")
      val prlStats = spark.sparkContext.parallelize(dsStats.collect.toSeq,1)
      prlStats.saveToCassandra("msk_arm_lead", "t_data_stats", SomeColumns("table_name","ddate", "id_pok", "id_oiv", "row_count", "insert_dur_ms"))


  }

  val t2_common = System.currentTimeMillis
  logger.info("----------------------------------------------------------")
  logger.info("                                                          ")
  logger.info(" > T_DATA COMMON DURATION = "+(t2_common - t1_common) + " ms. ")
  logger.info("                                                          ")
  logger.info("----------------------------------------------------------")



  val t1_tkeys = System.currentTimeMillis
  val dsTkeysDdateOiv = getDistinctDDatesIDOiv()

  logger.info("--------------------  dsTkeysDdateOiv.count()="+dsTkeysDdateOiv.count())

  dsTkeysDdateOiv filter(r => r.DDATE == 20180601 && Seq(1001,2003).contains(r.ID_OIV)) foreach {
    thisRow =>
      val t1_tkeys_data = System.currentTimeMillis
     //logger.info("here will get data from t_keys for id_oiv=" + thisRow.id_oiv)
      val dsTkeys = getTKeysData(thisRow.DDATE, thisRow.ID_OIV)
      val tKeysDataCnt = dsTkeys.count()
      //logger.info(" <<<<<<<  dsTkeys.count() = "+tKeysDataCnt)

      val prlStats = spark.sparkContext.parallelize(dsTkeys.collect.toSeq,1)

      prlStats.saveToCassandra("msk_arm_lead", "t_keys", SomeColumns(
        "ddate","ddate_actual","id_row","id_oiv","id_org","id_class_grbs","id_gp","id_budget","id_industry",
        "id_territory","id_uk","id_serv_work","id_msp","id_pref","id_income_budget","id_gu_type"))

      val t2_tkeys_data = System.currentTimeMillis

      spark.sparkContext.parallelize(Seq(new T_DATA_STATS("T_KEYS", thisRow.DDATE, 0.toInt, thisRow.ID_OIV , tKeysDataCnt, (t2_tkeys_data - t1_tkeys_data)))
        .toDF("table_name", "ddate", "id_pok", "id_oiv", "row_count", "insert_dur_ms").collect().toSeq,1)
        .saveToCassandra("msk_arm_lead", "t_data_stats", SomeColumns("table_name","ddate", "id_pok", "id_oiv", "row_count", "insert_dur_ms"))

      logger.info("--------------------------------------------------------------------------")
      logger.info("                                                                          ")
      logger.info("                                                                          ")
      logger.info(" DDATE="+thisRow.DDATE+" ID_OIV = "+ thisRow.ID_OIV+" ROWS = "+tKeysDataCnt)
      logger.info(" ONE PART T_KEYS DURATION = "+(t2_tkeys_data - t1_tkeys_data) + " ms.     ")
      logger.info("                                                                          ")
      logger.info("--------------------------------------------------------------------------")

  }

  val t2_tkeys = System.currentTimeMillis
  logger.info("----------------------------------------------------------")
  logger.info("                                                          ")
  logger.info(" > T_KEYS COMMON DURATION = "+(t2_tkeys - t1_tkeys) + " ms.   ")
  logger.info("                                                          ")
  logger.info("----------------------------------------------------------")

  logger.info("================== SUMMARY ========================================")
  logger.info(" DURATION :"+ ((t2_common - t1_common).toDouble + (t2_tkeys - t1_tkeys).toDouble).toString + " ms.")
  logger.info("================== END [OraToCass] ================================")
}
