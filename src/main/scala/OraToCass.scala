import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

//import com.datastax.spark.connector.cql.CassandraConnectorConf
//import com.datastax.spark.connector.rdd.ReadConf
//import com.datastax.spark.connector._

object otocLogg extends Serializable {
  @transient lazy val log = LoggerFactory.getLogger(getClass.getName)
}

object OraToCass extends App {
  otocLogg.log.info("BEGIN [OraToCass]")
  val url_string       = "jdbc:oracle:thin:"+"MSK_ARM_LEAD"+"/"+"MSK_ARM_LEAD"+"@//"+"10.127.24.11:1521/test"

  case class DDATE_POK(ddate :Int,id_pok :Int)
  case class T_DATA_ROW(ddate: Int, id_pok :Int, id_row :String, sval :String)
  case class T_DATA_TINY_ROW(id_row :String, sval :String)
  case class T_DATA_STATS(TABLE_NAME :String, DDATE: Int, ID_POK :Int, ID_OIV :Long, ROW_COUNT :Long, INSERT_DUR_MS :Double)

  val spark = SparkSession.builder()
    .master("spark://172.18.16.39:7077"/*"local[*]"*/)
    .appName("oratocass")
    .config("spark.cassandra.connection.host", "10.241.5.234")
    //.config("spark.cassandra.output.concurrent.writes","3")
    //.config("spark.cassandra.output.consistency.level","LOCAL_ONE")
    .config("spark.jars", "C:\\oratocass\\target\\scala-2.11\\oratocass_2.11-1.0.jar")
    .getOrCreate()

  import com.datastax.spark.connector.cql.CassandraConnectorConf
  import org.apache.spark.sql.cassandra._
  spark.setCassandraConf("cass cluster", CassandraConnectorConf.ConnectionHostParam.option("10.241.5.234"))

  import spark.implicits._

  def dsDdatesPoksFiltered = {
    val ds= spark
      .read.format("jdbc")
      .option("url", url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user", "MSK_ARM_LEAD").option("password", "MSK_ARM_LEAD")
      .option("dbtable", s"(select distinct DDATE,ID_POK from t_data order by 1,2)")
      .option("numPartitions", "1")
      .option("customSchema", "DDATE INT,ID_POK INT")
      .load()
    ds.as[DDATE_POK]
  }

  def getTDataByDDateIDPok(inDDate :Int, inIDPok :Long) = {
  /*
    val ds = spark.read.format("jdbc")
      .option("customSchema","DDATE INT, ID_POK INT, ID_ROW String, SVAL String")
   */

    val ds = spark.read.format("jdbc")
      .option("url",url_string)
      .option("dbtable", "Javachain_Oracle.Javachain_log")
      .option("user","MSK_ARM_LEAD").option("password", "MSK_ARM_LEAD")
      .option("dbtable",s"(select /*+ PARALLEL(4) */ id_row,val as sval from T_DATA where ddate="+inDDate+" and id_pok="+inIDPok+" order by id_row)")
      .option("numPartitions", "1")
      .option("fetchSize", "30000")
      .load()
    ds.as[T_DATA_TINY_ROW]
  }


  val t1_common = System.currentTimeMillis
  val ds =  dsDdatesPoksFiltered
  val dsFiltered = ds.filter(r => r.ddate == 20170601 && Seq(2096).contains(r.id_pok))

  dsFiltered.collect().toSeq foreach {
    val t1 = System.currentTimeMillis
    thisRow =>
    val ds = getTDataByDDateIDPok(thisRow.ddate,thisRow.id_pok)
      /*
       otocLogg.log.info("== > ============================================================================")
       otocLogg.log.info(" ======== "+thisRow.ddate+"   "+thisRow.id_pok+"  ds.count() = " + ds.count())
       otocLogg.log.info("== < ============================================================================")
     */
      val rCount = ds.count()

      import org.apache.spark.sql.functions.lit

      ds.toDF("id_row", "sval")
        .withColumn("ddate",lit(thisRow.ddate))
        .withColumn("id_pok",lit(thisRow.id_pok))
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "t_data", "keyspace" -> "msk_arm_lead", "cluster" -> "cass cluster"))
        .mode(org.apache.spark.sql.SaveMode.Append).save

      val t2 = System.currentTimeMillis

      val dsStats = Seq(new T_DATA_STATS("T_DATA", thisRow.ddate, thisRow.id_pok, 0.toLong , rCount, (t2 - t1)))
        .toDF("table_name", "ddate", "id_pok", "id_oiv", "row_count", "insert_dur_ms")
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "t_data_stats", "keyspace" -> "msk_arm_lead", "cluster" -> "cass cluster"))
        .mode(org.apache.spark.sql.SaveMode.Append).save
  }

  val t2_common = System.currentTimeMillis
  otocLogg.log.info("================== SUMMARY ========================================")
  otocLogg.log.info(" DURATION :"+ ((t2_common - t1_common)/1000.toDouble) + " sec.")
  otocLogg.log.info("================== END [OraToCass] ================================")
}
