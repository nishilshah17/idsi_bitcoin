/**
  * Created by NishilShah on 3/29/17.
  */
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import java.text.SimpleDateFormat;
import java.io._;

object Blockchain {
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Blockchain").setMaster("local")
    val sc = new SparkContext(conf)

    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy")
    val newFormat = new SimpleDateFormat("MM yyyy")

    val blockFilePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_data/*-blocks-r-00000"
    val transactionFilePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_data/*-transactions-r-00000"
    val outputFilePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_output/new_addresses.txt"

    val blockLines = sc.textFile(blockFilePath)
    val transactionLines = sc.textFile(transactionFilePath)
    val blocks = blockLines.map(line => line.split(",")).map(arr => (arr(0), dateFormat.parse(arr(3))))
    val transactions = transactionLines.map(line => line.split(","))
      .flatMap(arr => arr(6).split(":").map(address => (arr(0), address))).filter(tx => tx._2 != "null")
    val addressFirstSeen = blocks.join(transactions).map(entry => (entry._2._2, entry._2._1))
      .reduceByKey((a, b) => if(a.before(b)) a else b).map(entry => (entry._1, newFormat.format(entry._2)))
    val addressesPerMonth = addressFirstSeen.map(entry => (entry._2, 1)).reduceByKey(_ + _).collect()

    printToFile(new File(outputFilePath)) { printer =>
      addressesPerMonth.foreach(printer.println)
    }
  }
}
