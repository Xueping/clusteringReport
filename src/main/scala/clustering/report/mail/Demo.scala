package clustering.report.mail

object Demo {
  import mail._

  
  
  def main(args: Array[String]):Unit = {
   
    send a new Mail (
        from = "xueping.peng@outlook.com" -> "Xueping Peng",
        to = "pengxueping@gmail.com",
        subject = "offline Clutering Report",
        message = "Here is the presentation with the stuff we're going to for the next five years.",
        attachment = new java.io.File("./resource/packed/offline_clustering_report.zip")
      )
   
  }
}