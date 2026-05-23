# 25118_3 README

## Περιγραφή
Ο φάκελος έχει τρία μέρη:
1. `Q1` – Spark Scala parser για αρχείο Shakespeare (`pg100.txt`) και εξαγωγή σε CSV: plays/scenes/characters/lines.
2. `Q2` – `import.cypher` για εισαγωγή των CSV σε Neo4j γράφο.
3. `Q3` – 6 queries Cypher (παραδείγματα ανάλυσης στα σενάρια `Hamlet`, `Macbeth`, κλπ.).

## Τι πρέπει να γίνει
- Εκτέλεση `ShakespeareParser.scala` με Spark (local) και εισαγωγή csv από `output_csv`.
- Εκτέλεση Neo4j (π.χ. Docker με Neo4j Desktop) και τρέξιμο των εντολών `import.cypher`.
- Εκτέλεση `query1..query6.txt` στο Neo4j Browser και καταγραφή αποτελεσμάτων.
- Προσθήκη validation (count rows, μοναδικότητα, indexes).

## Τι έχει γίνει (analyzed code)
- `ShakespeareParser.scala`: ανάλυση play/act/scene/character/line με regex, διατηρεί δομές, γράφει 4 CSV αρχεία σε `output_csv`.
- `import.cypher`: καθαρίζει γράφο, δημιουργεί indexes, φορτώνει CSV και δημιουργεί κόμβους/συσχετίσεις (Play/Act/Scene/Character/Line).
- `query1.txt` etc: επιλογές Cypher, με αποτελέσματα (όπως line count του Hamlet, χαρακτήρες του Act II β, γραμμές Macbeth κλπ.).

## Σημειώσεις
- Υπάρχει ακόμα πιθανότητα να χρειαστεί προσαρμογή των paths `file:///plays.csv` σε Neo4j import directory.
- Τα queries που είναι ήδη στο αρχείο είναι δείγματα και έχουν (στα comments) sample output.
