/**
  * Created by NishilShah on 3/29/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object AnalyzeBlockchain {
  private def setSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("AnalyzeBlockchain").setMaster("local")
    new SparkContext(conf)
  }

  private def printUsageError() = {
    println("Arguments: <job> <input-path> <output-path>")
    println("Jobs: NewAddresses, AddressReuse")
  }

  def main(args: Array[String]): Unit = {
    val job = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val sc = setSparkContext()

    if(job == "NewAddresses") {
      NewAddresses.run(inputPath, outputPath, sc)
    } else if(job == "AddressReuse") {
      AddressReuse.run(inputPath, outputPath, sc)
    } else {
      printUsageError()
    }
  }
}
