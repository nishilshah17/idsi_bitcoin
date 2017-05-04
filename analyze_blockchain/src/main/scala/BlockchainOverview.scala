/**
  * Created by NishilShah on 5/2/17.
  */
import org.apache.spark.SparkContext

object BlockchainOverview {

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    //input
    val blocksPath = "hdfs://" + inputPath + "/blocks*"
    val transactionsPath = "hdfs://" + inputPath + "/transactions*"
    //output
    val outputFilePath = outputPath + "/overview.txt"
    val writer = AnalyzeBlockchain.printWriter(outputFilePath)

    //import data
    val blocksRDD = sc.textFile(blocksPath)
    val transactionsRDD = sc.textFile(transactionsPath)
    //count
    val blockCount = blocksRDD.count()
    val transactionCount = transactionsRDD.count()

    //print output
    var overview = "Blocks: " + blockCount + "\n"
    overview += "Transactions: " + transactionCount + "\n"
    try {
      writer.write(overview)
    } finally {
      writer.close()
    }
  }

}
