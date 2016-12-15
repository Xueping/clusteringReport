package clustering.report.clustering

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import clustering.report.html.Visualization
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

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

    val cluster : ClusteringAPCData =  new ClusteringAPCData()
    val data = cluster.obtainClusters()
    data.saveAsTextFile("/Users/xuepingpeng/Dropbox/team/HoD/interactiveClustering/output.txt");
   
  }
  
}