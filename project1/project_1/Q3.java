//Διαχείριση Δεδομένων Μεγάλου Όγκου 2025-2026
//Αρχείο Q3
//Εκφώνηση
// Εκτυπώστε τις λέξεις που εμφανίζονται στο pg100.txt και δεν εμφανίζονται
// στο pg46.txt. Μην εκτυπώσετε αχρείαστη πληροφορία (πχ συχνότητα εμφάνισης), μόνο τις λέξεις.

// Υπάρχουν αναλυτικά σχόλια για την αντικατάσταση αχείου Readme


import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3 {
    public static class FilterMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
/*
Κλάση Mapper: FilterMapper
Διαβάζει την έξοδο του Q2 γραμμή προς γραμμή.
--Είσοδος (Input):
-Key: Object (Offset γραμμής).
-Value: Text (Μια γραμμή από την έξοδο του Q2, π.χ. 
"the<TAB>pg100:50, el_quijote:20, pg46:25" ή
"thou<TAB>pg100:37")
Εφαρμόζει τη λογική φιλτραρίσματος.
--Έξοδος (Output) - Ενδιάμεσα ζεύγη (key, value):
 -Key: Text (Η λέξη που ικανοποιεί το κριτήριο, π.χ. "thou").
-Value: Text (Ένα κενό string, απλά για να υπάρχει τιμή).
*/
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Parsing της γραμμής εισόδου (από Q2)
            //Αναμενόκμενες γραμμές με μορφή: word<TAB>file1:count, file2:count, ...
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            //Η είσοδος είναι "word<TAB>filelist"
            //Χρησιμοποιείται το split("\\s+", 2) για να σπάσει στο πρώτο κενό (TAB)
            //και να κρατήσει το υπόλοιπο (τη λίστα αρχείων) ως ένα κομμάτι.
            String[] parts = line.split("\\s+", 2); // split σε λέξη και υπόλοιπο
            if (parts.length < 2) return;

            String w = parts[0]; //Η λέξη (π.χ. "the")
            String files = parts[1]; //Η λίστα (π.χ. "pg100:50, el_quijote:20, pg46:25")

            //Φιλτράρισμα
            //Ελέγχουμε αν η λίστα αρχείων περιέχει τα ονόματα που μας ενδιαφέρουν.
            boolean inPg100 = files.contains("pg100.txt");
            boolean inPg46 = files.contains("pg46.txt");
            //Παραγωγή εξόδου (ΜΟΝΟ αν ισχύει η συνθήκη)
            //Συνθήκη α υπάρχει στο pg100 ΚΑΙ ΔΕΝ υπάρχει στο pg46
            if (inPg100 && !inPg46) {
                word.set(w);
                context.write(word, new Text(""));
            }
        }
    }

/**
Κλάση Reducer: IdentityReducer
--Είσοδος (Input):
-Key: Text (Μια λέξη που πέρασε το φίλτρο, π.χ. "thou").
-Values: Iterable<Text> (Μια λίστα από κενά strings, π.χ. ["", "", ""]).
Ακόμα κι αν πολλοί Mappers στείλουν την ίδια λέξη ("thou"), ο Reducer 
θα κληθεί μόνο μία φορά για το "thou" και θα το γράψει μία φορά στην έξοδο.
--Έξοδος (Output) 
-Τελικό αποτέλεσμα:
-Key: Text (Η μοναδική λέξη, π.χ. "thou").
-Value: null (Δεν γράφουμε τιμή, μόνο το κλειδί).
*/
    public static class IdentityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            context.write(key, null); // εκτυπώνεται μόνο η λέξη
        }
    }

    //Κλάση main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q3 Filter pg100-not-pg46");
        job.setJarByClass(Q3.class);
        //Ορισμός κλάσεων Jar, Mapper, Reducer
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(IdentityReducer.class);

        //Ορισμός τύπων εξόδου (Reducer)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //Παρόλο που γράφουμε null, ο τύπος πρέπει να ταιριάζει


        //Ορισμός διαδρομών εισόδου/εξόδου
        //args[0] = Φάκελος εισόδου ΠΡΕΠΕΙ να είναι ο φάκελος εξόδου του Q2.
        //(π.χ. /user/root/output_q2)
        FileInputFormat.addInputPath(job, new Path(args[0])); // στο δικό μου /user/root/output_q2
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // στο δικό μου /user/root/output_q3

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
