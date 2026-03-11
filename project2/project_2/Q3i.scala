import org.apache.spark.{SparkConf, SparkContext}

object Q3i {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Q3i").setMaster("local[*]")
    val sc = new SparkContext(conf)

    def getUniqueWords(path: String) = {
      sc.textFile(path)
        .flatMap(line => line.toLowerCase.split("\\W+"))
        .filter(_.nonEmpty)
        .distinct()
    }

    val wordsPg100 = getUniqueWords("input/pg100.txt")
    val wordsPg46 = getUniqueWords("input/pg46.txt")

    // Το subtract αφαιρεί τα στοιχεία του 2ου RDD από το 1ο
    val result = wordsPg100.subtract(wordsPg46).coalesce(1)

    result.saveAsTextFile("output_Q3i")
    sc.stop()
  }
}