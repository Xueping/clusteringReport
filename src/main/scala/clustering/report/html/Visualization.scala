package clustering.report.html

import scalatags.Text.all._
import java.io.File
import org.zeroturnaround.zip.ZipUtil
import java.io.FileInputStream
//import play.api.libs.json.Json
import org.apache.commons.lang.StringEscapeUtils
import org.apache.commons.io.FileUtils

object Visualization {
  
  
//  /**
// * @param file
// * @return
// */
//def generateJSON(file: String) = {
//    
//    val stream = new FileInputStream(file)
//    val json = try { Json.parse(stream) } finally { stream.close() }
//    json
//  }
//  
  
  def generateHtml(jsonStr : String) = {
    
     val htmlFrag = {
             html(
              head(
                  meta(httpEquiv:="Content_Type", content:="text/html; charset=UTF-8"),
                  link(tpe:="text/css",rel:="stylesheet", href:="./dependency-files/style.css"),
                  link(tpe:="text/css",rel:="stylesheet", href:="./dependency-files/bootstrap.min.css"),
                  script(tpe:="text/javascript",src:="./dependency-files/d3.layout.js.download"),
                  script(tpe:="text/javascript",src:="./dependency-files/d3pie.js"),
                  script(tpe:="text/javascript",src:="./dependency-files/jquery.js"),
                  script(tpe:="text/javascript",src:="./dependency-files/d3.min.js")
              ),
              body(
                  div( id:="header", "Offline Web-based Interactive Clustering Analysis Report"),
                  div( id:="body"),
                  div( id:="popup",
                      div( cls:="row",
                          div(cls:="col-md-4", id:="pie_gender"),
                          div(cls:="col-md-4", id:="pie_age"),
                          div(cls:="col-md-4", id:="pie_freq")
                          )
                   ),
                        
                  script(tpe:="text/javascript")(
                        //raw( "var flare = \'"+ generateJSON("./resource/source/clusterTree.json") +"\'; ")
                      raw( "var flare = \'"+ jsonStr +"\'; ")
                    ),
                   script(tpe:="text/javascript",src:="./dependency-files/clusterReport.js")
                   
                )
            )
      }
      
      htmlFrag.toString()
  }
  
  def packFiles( jsonStr : String) = {
    
    //Delete existing Files
    FileUtils.deleteQuietly(new File("./resource/packed/source.zip"))
    FileUtils.deleteQuietly(new File("./resource/packed/offline_clustering_report.zip"))
    
    //pack dependency files into a zip file
    ZipUtil.pack(new File("./resource/dependencies"), new File("./resource/packed/source.zip"));
    
    //put generated html file into existing source.zip and change name to offline_clustering_report.zip
    ZipUtil.addEntry(new File("./resource/packed/source.zip"), 
                    "offline_clustering_report.html", 
                    generateHtml(jsonStr).getBytes,
                    new File("./resource/packed/offline_clustering_report.zip"))
  }
  
  
 def main(args: Array[String]):Unit = {

//     packFiles(jsonStr)
   
  }
  
}