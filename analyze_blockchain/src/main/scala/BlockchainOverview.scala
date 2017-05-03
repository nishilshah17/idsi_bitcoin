/**
  * Created by NishilShah on 5/2/17.
  */
import java.io.File
import java.io.PrintWriter
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

object BlockchainOverview {

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)

    //input
    val blocksPath = "hdfs://" + inputPath + "/blocks*"
    val transactionsPath = "hdfs://" + inputPath + "/transactions*"
    //output
    val outputFilePath = new Path(outputPath + "/overview.txt")
    val output = fileSystem.create(outputFilePath)
    val writer = new PrintWriter(output)

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
