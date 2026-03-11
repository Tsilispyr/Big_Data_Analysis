import org.apache.spark.{SparkConf, SparkContext}

object Q3ii {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Q3ii").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Cache γιατί θα το χρησιμοποιήσουμε παρακάτω
    val input = sc.wholeTextFiles("input").cache()

    def extractWords(targetFileFragment: String) = {
      input
        //contains check είναι γρήγορο
        .filter { case (path, _) => path.contains(targetFileFragment) }
        .flatMap { case (_, content) => content.toLowerCase.split("\\W+") }
        .filter(_.nonEmpty) // Αφαιρούμε τα κενά strings
        .distinct()
    }

    val wordsPg100 = extractWords("pg100") // Δεν χρειάζεται το .txt αν το contains πιάνει το όνομα
    val wordsPg46 = extractWords("pg46")

    // Το subtract απαιτεί Shuffle, είναι ακριβό αλλά αναγκαίο
    val result = wordsPg100.subtract(wordsPg46).coalesce(1)

    result.saveAsTextFile("output_Q3ii")
    sc.stop()
  }
}