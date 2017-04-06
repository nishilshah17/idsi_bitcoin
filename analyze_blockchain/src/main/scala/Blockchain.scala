/**
  * Created by NishilShah on 3/29/17.
  */
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object Blockchain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Blockchain").setMaster("local")
    val sc = new SparkContext(conf)

    val filePath = "/Users/NishilShah/workspace/idsi_bitcoin/sample_data/January-2013-r-00000"
    val lines = sc.textFile(filePath)
    val transactions = lines.map(line => line.split(",")).filter(arr => arr(2) == "0").map(arr => (arr(1), arr))
    val count = transactions.count()

    println(count)
  }
}
