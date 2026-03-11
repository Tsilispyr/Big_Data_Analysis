import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions.col
import scala.collection.mutable.ListBuffer

object ShakespeareParser {

  // Case classes
  case class Play(title: String, startLine: Long, var endLine: Long)
  case class Scene(play: String, act: String, scene: String, startLine: Long, var endLine: Long)
  case class Character(name: String, play: String)
  case class Line(character: String, text: String, act: String, scene: String, lineNum: Long, play: String)

  def main(args: Array[String]): Unit = {
    
    // --- Hadoop ---
    System.setProperty("hadoop.home.dir", "C:\\hadoop") 

    val spark = SparkSession.builder()
      .appName("Shakespeare Parser")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    
    // Μειώνουμε τον θόρυβο στα logs
    spark.sparkContext.setLogLevel("ERROR")

    println("--- Ξεκινάει η επεξεργασία του pg100.txt ---")

    val rawData = spark.sparkContext.textFile("input/pg100.txt")
      .zipWithIndex()
      .map { case (line, id) => (line.trim, id + 1) }
      .filter(_._1.nonEmpty)
      .collect()

    val plays = ListBuffer[Play]()
    val scenes = ListBuffer[Scene]()
    val characters = ListBuffer[Character]()
    val lines = ListBuffer[Line]()

    var currentPlay = ""
    var currentAct = ""
    var currentScene = ""
    var currentCharacter = ""
    var playHasAct = false    
    var sceneHasLines = false



    // --- 1. REGEX ---
    // (?i) : Αγνοεί πεζά/κεφαλαία
    // \s* : Αγνοεί αρχικά κενά
    // [IVX]+ : Πιάνει λατινικούς αριθμούς
    val actRegex = """(?i)^\s*ACT\s+[IVX]+.*""".r
    val sceneRegex = """(?i)^\s*SCENE\s+[IVX]+.*""".r
    val stageDirectionRegex = """^\[.*\]$""".r
    
    // Regex για ονόματα (Κεφαλαία που τελειώνουν σε τελεία)
    val charRegex = "^([A-Z]+(?: [A-Z]+)*)\\.$".r 

    // --- 2. ΣΥΝΑΡΤΗΣΗ ΚΑΘΑΡΙΣΜΟΥ ---
    def cleanHeader(text: String): String = {
      text.split("\\.")(0)  // Κρατάμε το αριστερό μέρος
          .replaceAll("[^a-zA-Z0-9IVX ]", "") // Αφαιρεί σύμβολα
          .replaceAll("\\s+", " ") // Αφαιρεί κενα
          .replaceAll("^\\s+", "") // Αφαιρεί αρχικά κενά
          .trim
          .toUpperCase
    }

    def isPlayTitle(s: String): Boolean = {
    val t = s.trim
    // πρέπει να είναι ΟΛΟ κεφαλαία
    t == t.toUpperCase &&
    // να έχει αρκετό μήκος (π.χ. "EXIT" να απορριφθεί)
    t.length >= 12 &&
    // να έχει τουλάχιστον 3 λέξεις (π.χ. HAMLET απορρίπτεται)
    t.split("\\s+").length >= 3 &&
    // να μην είναι ACT ή SCENE
    !t.startsWith("ACT ") &&
    !t.startsWith("SCENE ") &&
    // να μην τελειώνει με τελεία, underscore, κόμμα κλπ.
    !t.matches(".*[\\.,_’!?:;]$") &&
    // μόνο γράμματα, spaces και απλή απόστροφο
    t.matches("[A-Z\\s',-]+") &&
    // όχι THE END
    !t.contains("THE END")
    }


    // --- 3. LOOP ΕΠΕΞΕΡΓΑΣΙΑΣ ---
    for ((text, lineNum) <- rawData) {
      
      // Έλεγχος Τίτλου Έργου
      if (isPlayTitle(text)) {
        // κλείνουμε τελευταίο Scene
        if (scenes.nonEmpty && sceneHasLines) {
          scenes.last.endLine = lineNum - 1
        }

        // κλείνουμε προηγούμενο Play
        if (plays.nonEmpty) {
          if (!playHasAct) {
            plays.remove(plays.size - 1)   // πετάμε ποιήματα
          } else {
            plays.last.endLine = lineNum - 1
          }
        }

        // ξεκινά νέο Play
        currentPlay = text
        plays += Play(currentPlay, lineNum, 0)
        currentAct = ""
        currentScene = ""
        currentCharacter = ""
        playHasAct = false
      }


      // Έλεγχος ACT 
      else if (actRegex.findFirstIn(text).isDefined) {
        val cleaned = cleanHeader(text)
        currentAct = if (cleaned.nonEmpty) cleaned else "UNKNOWN_ACT"
        playHasAct = true
      }

      // Έλεγχος SCENE
      else if (sceneRegex.findFirstIn(text).isDefined) {
        //  if (scenes.nonEmpty && scenes.last.play == currentPlay)
        if (scenes.nonEmpty && sceneHasLines) {
          scenes.last.endLine = lineNum - 1
        }
        currentScene = cleanHeader(text)
        sceneHasLines = false
      }


      // Έλεγχος Χαρακτήρων / Γραμμών
        else {
        text match {
          case charRegex(name) => 
            // Να μην πιάσει το "ACT II" ή "SCENE I" ως όνομα χαρακτήρα
            if (!name.toUpperCase.startsWith("ACT ") && !name.toUpperCase.startsWith("SCENE ")) {
               currentCharacter = name
               if (currentPlay.nonEmpty) characters += Character(name, currentPlay)
            }
           case _ =>
          // Αγνοούμε σκηνικές οδηγίες
          if (stageDirectionRegex.findFirstIn(text).isDefined) {
          // DO NOTHING
          }
          // Αν έχουμε χαρακτήρα, τότε η γραμμή είναι ατάκα
          else if (currentCharacter.nonEmpty &&
            currentPlay.nonEmpty &&
            currentScene.nonEmpty) {
            val actValue = if (currentAct.nonEmpty) currentAct else "PROLOGUE"      
            if (!sceneHasLines) {
              scenes += Scene(currentPlay, actValue, currentScene, lineNum, 0)
              sceneHasLines = true
            }

            lines += Line(currentCharacter, text, actValue, currentScene, lineNum, currentPlay)
          }        }
      }
    }

    // if (plays.nonEmpty) plays.last.endLine = rawData.last._2
    if (plays.nonEmpty) { // πετάμε ποιήματα στο τελος, Αν το τελευταίο “Play” δεν είχε ACT, δεν μένει μέσα.
      if (!playHasAct) plays.remove(plays.size - 1)
      else plays.last.endLine = rawData.last._2
    }
    if (scenes.nonEmpty) scenes.last.endLine = rawData.last._2

    println(s"Βρέθηκαν ${plays.size} έργα, ${scenes.size} σκηνές, ${lines.size} γραμμές.")

    //Δεν μένει Play χωρίς ACT
    val validPlays = scenes.map(_.play).toSet
    val filteredPlays = plays.filter(p => validPlays.contains(p.title))
    filteredPlays.toSeq.toDF()

    //Δεν μένει Scenes και Lines χωρίς Play
    val filteredScenes = scenes.filter(s => validPlays.contains(s.play))
    val filteredLines  = lines.filter(l => validPlays.contains(l.play))
    filteredScenes.toSeq.toDF()
    filteredLines.toSeq.toDF()  


    // --- ΕΓΓΡΑΦΗ CSV ---

    // 1. plays.csv
    // Header names: title, start_line, end_line
    filteredPlays.toSeq.toDF()
      .select(col("title"), col("startLine").as("start_line"), col("endLine").as("end_line"))
      .dropDuplicates("title").coalesce(1)
      .write.mode(SaveMode.Overwrite).option("header", "true").csv("output_csv/plays")

    // 2. scenes.csv
    // Header names: play, act, scene, start_line, end_line
    filteredScenes.toSeq.toDF()
      .select(col("play"), col("act"), col("scene"), col("startLine").as("start_line"), col("endLine").as("end_line"))
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).option("header", "true").csv("output_csv/scenes")

    // 3. characters.csv
    // Header names: name, play
    characters.toSeq.toDF()
      .select(col("name"), col("play"))
      .dropDuplicates("name", "play").coalesce(1)
      .write.mode(SaveMode.Overwrite).option("header", "true").csv("output_csv/characters")

    // 4. lines.csv
    // Header names: character, text, act, scene, line_number, play  <-- ΠΡΟΣΤΕΘΗΚΕ ΤΟ PLAY
    filteredLines.toSeq.toDF()
      .select(
        col("character"), 
        col("text"), 
        col("act"), 
        col("scene"), 
        col("lineNum").as("line_number"),
        col("play")
      )
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).option("header", "true").csv("output_csv/lines")

    println("Επιτυχία! Τα αρχεία δημιουργήθηκαν σωστά.")
    spark.stop()
  }
}