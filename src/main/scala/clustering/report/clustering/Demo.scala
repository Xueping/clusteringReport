package clustering.report.clustering

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Demo {
 
  val appName: String = "ClusteringExample";

    // Initialize Spark configuration & context
    val sparkConf: SparkConf = new SparkConf().setAppName(appName)
      .setMaster("local[10]")
//      .set("spark.executor.memory", "2gb")
      .set("spark.driver.memory","60gb")
          .set("spark.rdd.compress","true")
//          .set("spark.memory.useLegacyMode","true")
          .set("spark.storage.memoryFraction", "0.9")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.max","2000mb")
          .set("spark.kryoserializer.buffer","64mb")
          .set("spark.default.parallelism","32")
          .set("spark.eventLog.enabled","true")

    val sc: SparkContext = new SparkContext(sparkConf);
  
   var rawData = sc.textFile("/home/xuepeng/git/clusteringReport/resource/source/demo_pre.csv")
   
   val header = rawData.first() 
   rawData = rawData.filter(row => row != header)   
    
   val newData = rawData.map(_.split(",")).map(
        x => {
        val b = x.toBuffer
        b.remove(1)
        Vectors.dense(b.map(_.toDouble).toArray)})
        
   val model = KMeans.train(newData, k=5,100)
   val predict = model.predict(newData)

}