# 25118_1 README

## Περιγραφή
1. Ανεστραμμένο ευρετήριο (`Q1`) ώστε κάθε λέξη να δείχνει σε ποια αρχεία εμφανίζεται.
2. Επέκταση με συχνότητα εμφάνισης ανά αρχείο για λέξεις >= 3 χαρακτήρων (`Q2`).
3. Φιλτράρισμα λέξεων που υπάρχουν στο `pg100.txt` αλλά όχι στο `pg46.txt` (`Q3`).

## Τι πρέπει να γίνει
- Ανέβασμα / εγκατάσταση περιβάλλοντος Hadoop (HDFS, YARN) και compilation με `javac`/`mvn`.
- Εκτέλεση:
  - `Q1`: input folder -> output folder
  - `Q2`: input folder -> output folder
  - `Q3`: output_q2 ως input και output_q3 ως output
- Έλεγχος των αποτελεσμάτων (συμπεριλαμβανομένης μορφοποίησης `word	filelist`).
- Προαιρετικά: προσθήκη Combiner για Q1/Q2 και optimization.

## Τι έχει γίνει (analyzed code)
- `Q1.java`: υλοποίηση Inverted Index με `mapper` που παράγει `(word, filename)` και `reducer` που συγκεντρώνει μοναδικά ονόματα αρχείων.
- `Q2.java`: υλοποίηση με in-mapper aggregation για κάθε γραμμή, πότε λέξεις 3+ χαρακτήρων, και reducer συνδυασμό `filename:count`.
- `Q3.java`: υλοποίηση φιλτραρίσματος με `FilterMapper` (επιλογή `pg100.txt` και όχι `pg46.txt`) και `IdentityReducer`.
