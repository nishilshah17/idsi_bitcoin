/**
  * Created by NishilShah on 3/29/17.
  */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter

object AnalyzeBlockchain {
  private def setSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("AnalyzeBlockchain").setMaster("local")
    conf.set("spark.local.dir", "/mnt/volume/tmp")
    new SparkContext(conf)
  }

  private def printUsageError() = {
    println("Arguments: <job> <input-path> <output-path>")
    println("Jobs: NewAddresses, AddressReuse")
  }

  def printWriter(outputPath: String): PrintWriter = {
    val path = new Path(outputPath)
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val output = fileSystem.create(path)
    new PrintWriter(output)
  }

  def inputAddressIndex(): Int = {
    6
  }

  def outputAddressIndex(): Int = {
    7
  }

  def main(args: Array[String]): Unit = {
    val job = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val sc = setSparkContext()

    if(job == "BlockchainOverview") {
      BlockchainOverview.run(inputPath, outputPath, sc)
    } else if(job == "NewAddresses") {
      NewAddresses.run(inputPath, outputPath, sc)
    } else if(job == "AddressUse") {
      AddressUse.run(inputPath, outputPath, sc)
    } else {
      printUsageError()
    }
  }
}
