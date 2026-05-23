# README

## Τι πρέπει να γίνει

- Εκτέλεση των 6 προγραμμάτων:
  - `Q1i.scala`: inverted index με local distinct και union.
  - `Q1ii.scala`: wholeTextFiles + unique words.
  - `Q2i.scala`: per-file count για λέξεις >=3 και reduceByKey.
  - `Q2ii.scala`: wholeTextFiles + groupBy στο local memory.
  - `Q3i.scala`: subtract (pg100 - pg46) πάνω σε distinct λέξεις.
  - `Q3ii.scala`: wholeTextFiles με distinct και subtract.
- Καταγραφή μετα-αποτελεσμάτων (πλήθος λέξεων, μεγέθη, ταχύτητα).

## Τι έχει γίνει (analyzed code)
- Όλα τα αρχεία Scala είναι πλήρως λειτουργικά, με ξεκάθαρη ροή:
  - `mapFile`/`processFile`/`extractWords` helpers.
  - `.coalesce(1)` για 1 αρχείο εξόδου σε κάθε query.
  - Έλεγχος φίλτρων και τονity logic (length>=3, distinct, subtraction).
- Η δομή προτείνει πως ο φάκελος περιλαμβάνει εξασκήσεις MapReduce vs Spark ίδιας λογικής.

## Σημειώσεις
- Δεν χρειάζεται mapreduce HDFS, πρόκειται για μικρά test με local Spark.
- Μπορεί να γίνει εκτέλεση σε `spark-shell` / `sbt` project.
