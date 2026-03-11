import org.apache.spark.{SparkConf, SparkContext}

object Q1i {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Q1i").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // διαβάζει, καθαρίζει και κρατάει μοναδικές λέξεις
    def mapFile(path: String, filename: String) = {
      sc.textFile(path)
        .flatMap(line => line.toLowerCase.split("\\W+"))
        .filter(_.nonEmpty)
        .distinct() // Local distinctΜειώνει τα δεδομένα πριν το union
        .map(word => (word, filename))
    }

    val rdd1 = mapFile("input/pg100.txt", "pg100")
    val rdd2 = mapFile("input/pg46.txt", "pg46")
    val rdd3 = mapFile("input/el_quijote.txt", "el_quijote")



    // Ένωση και Reduce
    val result = rdd1.union(rdd2).union(rdd3)
      .reduceByKey((acc, file) => acc + ", " + file)
      .coalesce(1) // Output σε 1 αρχείο 

    result.saveAsTextFile("output_Q1i")
    sc.stop()
  }
}