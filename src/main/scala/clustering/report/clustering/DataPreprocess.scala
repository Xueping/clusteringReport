package clustering.report.clustering

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import clustering.report.html.Visualization
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object DataPreprocess {
  
  def vectorizationCategricalData(normalizedData : RDD[String]) = {
     
     val protocols = normalizedData.map(_.split(",")(2)).distinct.collect.zipWithIndex.toMap

     val dataAndLabel = normalizedData.map {
        line =>val buffer = ArrayBuffer[String]()
        buffer.appendAll(line.split(","))
        val protocol = buffer.remove(2)
        val label = buffer.remove(1)
        val vector = buffer.map(_.toDouble)
        val newProtocolFeatures = new Array[Double](protocols.size)
        newProtocolFeatures(protocols(protocol)) = 1.0
        vector.insertAll(1, newProtocolFeatures)
        (vector.toArray,label)
    }
    val data = dataAndLabel.map(_._1)
   }
  
  
   def  obtainCluster() = {
     
        // Set application name
        val appName:String  = "ClusteringExample";
        
        // Initialize Spark configuration & context
        val sparkConf:SparkConf = new SparkConf().setAppName(appName)
                .setMaster("local[1]").set("spark.executor.memory", "1g");
        
        val sc:SparkContext = new SparkContext(sparkConf);
        
        sc
        
    
  }
  
  
  def main(args: Array[String]):Unit = {
    
    FileUtils.deleteQuietly(new File("./resource/source/output.txt"))

    val cluster : ClusteringAPCData =  new ClusteringAPCData()
    val data = cluster.obtainClusters()
  
    val model = KMeans.train(data, k=5,100)
    
    val WSSSE = model.computeCost(data)
    
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    val clusters = model.predict(data)
    
    
    clusters.saveAsTextFile("./resource/source/output.txt")
    
    val labels = clusters.zipWithIndex().map{case(v,i) => (i,v)}
    val labeledData = data.zipWithIndex().map{case(v,i) => (i,v)}.join(labels)
    
    
//    val appName: String = "ClusteringExample";
//
//    // Initialize Spark configuration & context
//    val sparkConf: SparkConf = new SparkConf().setAppName(appName)
//      .setMaster("local[10]")
////      .set("spark.executor.memory", "2gb")
//      .set("spark.driver.memory","60gb")
//          .set("spark.rdd.compress","true")
////          .set("spark.memory.useLegacyMode","true")
//          .set("spark.storage.memoryFraction", "0.9")
//          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//          .set("spark.kryoserializer.buffer.max","2000mb")
//          .set("spark.kryoserializer.buffer","64mb")
//          .set("spark.default.parallelism","32")
//          .set("spark.eventLog.enabled","true")
//
//    val sc: SparkContext = new SparkContext(sparkConf);
//    
//    val rawData = sc.textFile("./resource/source/demo_pre.csv")
//    
//    val newData = rawData.map(_.split(",")).map(
//        x => {
//        val b = x.toBuffer
//        b.remove(1)
//        Vectors.dense(b.map(_.toDouble).toArray)})
    

//    data.map(f => f.mkString(" ")).saveAsTextFile("./resource/source/output.txt");
   
//    val str:String = "2,200005,200067,200060,200059,1,0,4,1"
//    println(str.split(",")(0))
  }
  
}