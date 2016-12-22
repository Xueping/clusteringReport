package clustering.report.clustering

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object Demo {
  
  import org.apache.spark.mllib.clustering.KMeans
  import org.apache.spark.mllib.linalg.Vectors
  
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