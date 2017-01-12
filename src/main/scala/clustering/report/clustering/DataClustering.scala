package clustering.report.clustering

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import clustering.report.html.Visualization


object DataClustering {
     
   def clustering(data : RDD[(Vector, String)]):String  = {
     
      val builder = StringBuilder.newBuilder
      
      val model = KMeans.train(data.map(_._1), k=5,10)
      
      //get partition of current data set 
      val clusters = model.predict(data.map(_._1)) 
      
      //index each row, the format is (index, cluster)
      val labels = clusters.zipWithIndex().map{case(v,i) => (i,v)} 
      
      //join by index to let each row map to a cluster
      val labeledData = labels.join(data.zipWithIndex().map{case(v,i) => (i,v)})

      //records in each cluster 
      val clusteredData = labeledData.map(f => f._2).groupByKey()
      
      //convert into cluster id, record number, and records
      val ids = clusteredData.map{f => 
         val clust = f._1
         val points = f._2.toList
         (clust,points.size,points)    
      }.collect()
      
      for( a <- 0 until ids.size ){
          val clust = ids(a)._1
          val size = ids(a)._2
          val newPoints = clusteredData.context.parallelize(ids(a)._3)
           
          val statisticEthnicity = newPoints.map(_._2.split(",")(1)).map { x => (x,1) }.reduceByKey(_+_).collect()
          val statisticPayor = newPoints.map(_._2.split(",")(2)).map { x => (x,1) }.reduceByKey(_+_).collect()
          val statisticAdmission = newPoints.map(_._2.split(",")(3)).map { x => (x,1) }.reduceByKey(_+_).collect()
            
          builder.append("{\"name\":").append(size).append(",\"clusterId\":").append(clust).append(",\"eth\":{")
          
          for( item <- statisticEthnicity)(
             builder.append("\"e").append(item._1).append("\":").append(item._2).append(",")
          )
          
          builder.append("},\"payor\":{")
                
          for( item <- statisticEthnicity)(
            builder.append("\"p").append(item._1).append("\":").append(item._2).append(",")
          )
                
          builder.append("},\"adm\":{")

          for( item <- statisticEthnicity)(
             builder.append("\"a").append(item._1).append("\":").append(item._2).append(",")
          )
           
          if(ids(a)._2 > 100) {
             builder.append("},\"children\":[").append(clustering(newPoints)).append("]},")
           }
           else {
             builder.append("},\"size\":").append((math.random*500+500).toInt).append("},")
           }
           
      }
      
      return builder.toString()
   }
  
  
  def main(args: Array[String]): Unit  = {
    
    val cluster : DataProcessor =  new DataProcessor()
    val data = cluster.obtainPreprocessData()
    val statisticEthnicity = data.map(_._2.split(",")(1)).map { x => (x,1) }.reduceByKey(_+_).collect()
    val statisticPayor = data.map(_._2.split(",")(2)).map { x => (x,1) }.reduceByKey(_+_).collect()
    val statisticAdmission = data.map(_._2.split(",")(3)).map { x => (x,1) }.reduceByKey(_+_).collect()
  
        
     val builder = StringBuilder.newBuilder
     builder.append("{\"name\":").append(data.count()).append(",\"clusterId\": \"Initial Data\",\"eth\":{")
     
     for( item <- statisticEthnicity)
       
      (
           builder.append("\"e").append(item._1).append("\":").append(item._2).append(",")
      )
      builder.append("},\"payor\":{")
      
     for( item <- statisticEthnicity)
      (
           builder.append("\"p").append(item._1).append("\":").append(item._2).append(",")
      )
      builder.append("},\"adm\":{")
     
      for( item <- statisticEthnicity)
      (
           builder.append("\"a").append(item._1).append("\":").append(item._2).append(",")
      )
      builder.append("},\"children\":[")
      
    val josn = builder.append(clustering(data)).append("]}").toString().replaceAll(",]", "]").replaceAll(",}", "}")
    
    Visualization.packFiles(josn)

  }
  
}