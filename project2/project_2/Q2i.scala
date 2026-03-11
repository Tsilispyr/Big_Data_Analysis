import org.apache.spark.{SparkConf, SparkContext}

object Q2i {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Q2i").setMaster("local[*]")
    val sc = new SparkContext(conf)

    def processFile(path: String, filename: String) = {
      sc.textFile(path)
        .flatMap(line => line.toLowerCase.split("\\W+"))
        .filter(_.length >= 3) // Λέξεις >= 3 χαρακτήρες
        .map(word => (word, 1))
        .reduceByKey(_ + _) // Local Aggregation: Συμπιέζει τα δεδομένα ΠΡΙΝ το Union (Μέτρηση συχνότητας)
        .map { case (word, count) => (word, filename + ":" + count) }
    }

    val rdd1 = processFile("input/pg100.txt", "pg100")
    val rdd2 = processFile("input/pg46.txt", "pg46")
    val rdd3 = processFile("input/el_quijote.txt", "el_quijote")

    val result = rdd1.union(rdd2).union(rdd3)
      .reduceByKey((acc, curr) => acc + ", " + curr)
      .coalesce(1)

    result.saveAsTextFile("output_Q2i")
    sc.stop()
  }
}