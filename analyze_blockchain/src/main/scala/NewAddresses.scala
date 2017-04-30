/**
  * Created by NishilShah on 4/30/17.
  */
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

object NewAddresses {

  def run(inputPath: String, outputPath: String, sc: SparkContext) = {
    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val newFormat = new SimpleDateFormat("MM yyyy")

    //local input path: /Users/NishilShah/workspace/idsi_bitcoin/sample_data
    val blockFilePath = "hdfs://" + inputPath + "/blocks*"
    val transactionFilePath = "hdfs://" + inputPath + "/transactions*"
    //local output path: /Users/NishilShah/workspace/idsi_bitcoin/sample_output
    val outputFilePath = "hdfs://" + outputPath + "/new_addresses"

    //import data
    val blockLines = sc.textFile(blockFilePath)
    val transactionLines = sc.textFile(transactionFilePath)
    //split data by commas
    val blocks = blockLines.map(line => line.split(",")).map(arr => (arr(0), dateFormat.parse(arr(3))))
    val transactions = transactionLines.map(line => line.split(","))
    val outputAddresses = transactions.flatMap(arr => arr(7).split(":").map(address => (arr(0), address)))
      .filter(tx => tx._2 != "null")
    //get month each address was first used
    val addressFirstSeen = blocks.join(outputAddresses).map(entry => (entry._2._2, entry._2._1))
      .reduceByKey((a, b) => if(a.before(b)) a else b).map(entry => (entry._1, newFormat.format(entry._2)))
    //get number of new addresses per month
    addressFirstSeen.map(entry => (entry._2, 1)).reduceByKey(_ + _).repartition(1).saveAsTextFile(outputFilePath)
  }
}
