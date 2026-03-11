import org.apache.spark.{SparkConf, SparkContext}

object Q2ii {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Q2ii").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 1. Διάβασμα
    val input = sc.wholeTextFiles("input")

    val result = input.flatMap { case (path, content) =>
      // Παίρνουμε το filename ΜΙΑ φορά ανά αρχείο
      val filename = path.split("/").last.stripSuffix(".txt")
      
      // Τοπικό φιλτράρισμα και μέτρηση στη μνήμη (In-Memory Aggregation)
      // Αντί να βγάλουμε (the,1), (the,1)... βγάζουμε κατευθείαν (the, 500)
      content.toLowerCase.split("\\W+")
        .filter(_.length >= 3) // Λέξεις >= 3 χαρακτήρες
        .groupBy(identity) // Scala collection grouping (Ομαδοποίηση λέξεων στη ram)
        .mapValues(_.length) // Μέτρηση στη ram
        .toSeq
        .map { case (word, count) => (word, filename + ":" + count) }
    }
    // 2. Reduce: Ενώνουμε τα strings. 
    .reduceByKey((acc, curr) => acc + ", " + curr)
    .coalesce(1) // Ένα αρχείο εξόδου

    result.saveAsTextFile("output_Q2ii")
    sc.stop()
  }
}