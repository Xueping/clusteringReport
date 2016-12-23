package clustering.report.clustering

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }


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
   
   def clustering(data : RDD[Vector]):String  = {
     
      val builder = StringBuilder.newBuilder
      val model = KMeans.train(data, k=5,100)
      val clusters = model.predict(data)    
      val labels = clusters.zipWithIndex().map{case(v,i) => (i,v)}
      val labeledData = labels.join(data.zipWithIndex().map{case(v,i) => (i,v)})    
      val clusteredData = labeledData.map(f => f._2).groupByKey()
      val ids = clusteredData.map{f => 
         val clust = f._1
         val points = f._2.toList
         (clust,points.size,points)    
      }.collect()
      
     for( a <- 0 until ids.size -1){
           val clust = ids(a)._1
           
           if(ids(a)._2 >100) {
             val newPoints = clusteredData.context.parallelize(ids(a)._3)
             builder.append(clustering(newPoints))
           }
           else builder.append(ids(a)._3.toString())
      }
      
      return builder.toString()
   }
  
  
  def main(args: Array[String]):Unit = {
    
    FileUtils.deleteQuietly(new File("./resource/source/output.txt"))

    val cluster : ClusteringAPCData =  new ClusteringAPCData()
    val data = cluster.obtainClusters()
    
    println(clustering(data))
    
    
//    val model = KMeans.train(data, k=5,100)
//    val clusters = model.predict(data)    
//    val labels = clusters.zipWithIndex().map{case(v,i) => (i,v)}
//    val labeledData = labels.join(data.zipWithIndex().map{case(v,i) => (i,v)})    
//    val clusteredData = labeledData.map(f => f._2).groupByKey()
//    val ids = clusteredData.map{f => 
//       val clust = f._1
//       val points = f._2.toList
//       val newPoints = clusteredData.context.parallelize(points)
//       (clust,points.size,newPoints)    
//    }
    

    

    

//    data.map(f => f.mkString(" ")).saveAsTextFile("./resource/source/output.txt");
   
//    val str:String = "2,200005,200067,200060,200059,1,0,4,1"
//    println(str.split(",")(0))
  }
  
}