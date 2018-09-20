import org.apache.spark.{SparkConf, SparkContext}

object OraToCass extends App {

  //Create a SparkContext to initialize Spark
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

}
