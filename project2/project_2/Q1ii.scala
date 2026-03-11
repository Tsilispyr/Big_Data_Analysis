import org.apache.spark.{SparkConf, SparkContext}

object Q1ii {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Q1ii").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.wholeTextFiles("input")

    val result = input.flatMap { case (path, content) =>
      // Εξαγωγή ονόματος αρχείου από το full path
      // Υπολογισμός filename ΜΙΑ φορά ανά αρχείο
      val filename = path.split("/").last.stripSuffix(".txt")
      
      content.toLowerCase.split("\\W+")
        .filter(_.nonEmpty)
        .distinct // Local distinct για το κείμενο
        .map(word => (word, filename))
    }
    .reduceByKey((acc, file) => acc + ", " + file) // Shuffle Phase
    .coalesce(1)

    result.saveAsTextFile("output_Q1ii")
    sc.stop()
  }
}