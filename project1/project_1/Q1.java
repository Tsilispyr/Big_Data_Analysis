//Διαχείριση Δεδομένων Μεγάλου Όγκου 2025-2026
//Αρχείο Q1
//Εκφώνηση
//(Ανεστραμμένο ευρετήριο) Φτιάξτε ένα ή περισσότερα αρχεία που να
//καταγράφουν σε ποια αρχεία εμφανίζεται η κάθε λέξη. Για παράδειγμα, αν η λέξη “a” εμφανίζεται και
// στα τρία αρχεία, ενώ η λέξη “thou” εμφανίζεται μόνο στο pg100.txt, το αντίστοιχο output του
// προγράμματός σας θα πρέπει να είναι:
// a pg100, el_quijote, pg46
// thou pg100

// Υπάρχουν αναλυτικά σχόλια για την αντικατάσταση αχείου Readme

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {
/*
Κλάση Mapper: InvertedIndexMapper
Διαβάζει το αρχείο εισόδου γραμμή προς γραμμή.
-- Είσοδος (Input):
- Key: Object .
- Value: Text (Το περιεχόμενο μιας γραμμής κειμένου).
Σπάει κάθε γραμμή σε λέξεις (tokens), κάνει κανονικοποίηση (lowercase) 
και εντοπίζει το όνομα του αρχείου από το οποίο προέρχεται η γραμμή.
-- Έξοδος (Output) - Ενδιάμεσα ζεύγη (key, value):
- Key: Text (Η λέξη, π.χ. "thou").
- Value: Text (Το όνομα του αρχείου, π.χ. "pg100.txt").
Για κάθε εμφάνιση μιας λέξης σε ένα αρχείο, παράγει ένα ζεύγος (λέξη, όνομα_αρχείου).
π.χ. (thou, pg100.txt), (thou, pg100.txt), (a, pg100.txt), (a, pg46.txt)...
*/
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private final static Pattern WORD_BOUNDARY = Pattern.compile("\\W+");
        private Text word = new Text();
        private Text filename = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Λήψη ονόματος αρχείου
            // Χρησιμοποιούμε το 'context' για να βρούμε πληροφορίες σχετικές με το InputSplit,
             // και από εκεί παίρνουμε το path και το όνομα του αρχείου.
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            filename.set(fileName);

             //Επεξεργασία γραμμής       
            String line = value.toString().toLowerCase();

            // Tokenization για τμηματοποίηση
            //Κάθε γραμμή σπάει σε λέξεις (tokens) χρησιμοποιώντας το Pattern.
            //Σπάμε πολύ γενικά με βάση οτιδήποτε δεν είναι γράμμα ή αριθμός 
            String[] tokens = WORD_BOUNDARY.split(line);

            // 4. Παραγωγή ζευγών (key, value)
            for (String token : tokens) {
                if (token.length() > 0) { // Αγνόηση κενών tokens
                    word.set(token);
                    //  Γράφουμε το ζεύγος (λέξη, όνομα_αρχείου) στην έξοδο του Mapper.
                    context.write(word, filename);
                }
            }
        }
    }

/*
Κλάση Reducer: InvertedIndexReducer
Λαμβάνει τα ομαδοποιημένα δεδομένα από το στάδιο Shuffle/Sort.
--Είσοδος (Input):
-Key: Text (Μια μοναδική λέξη, π.χ. "thou").
-Values: Iterable<Text> (Μια λίστα με όλα τα ονόματα αρχείων 
που έστειλαν οι Mappers για αυτή τη λέξη, π.χ. ["pg100.txt", "pg100.txt", ...]).
Συγκεντρώνονται τα ονόματα των αρχείων για κάθε λέξη,  
κάθε όνομα αρχείου εμφανίζεται μόνο μία φορά.
--Έξοδος (Output) 
-Τελικό αποτέλεσμα:
-Key: Text (Η λέξη, π.χ. "thou").
-Value: Text (Η λίστα των μοναδικών αρχείων, π.χ. "pg100.txt").
*/
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 1.Χρήση HashSet για αποθήκευση μοναδικών ονομάτων αρχείων
            // Εάν κάτι έρθει πολλαπλές φορές, θα καταγραφεί μόνο μία φορά.
            Set<String> uniqueFiles = new HashSet<>();
            for (Text val : values) {
                uniqueFiles.add(val.toString());
            }

            // 2.Δημιουργία τελικού string
            // Συνένωση των μοναδικών ονομάτων αρχείων με ", "
            result.set(String.join(", ", uniqueFiles));

            // 3. Εγγραφή τελικού αποτελέσματος
            // Γράφουμε τη λέξη (key) και τη λίστα αρχείων (value) στην έξοδο.
            context.write(key, result);
        }
    }


// Κλάση main
// Ρυθμίζει και ξεκινάει το job MapReduce

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        //Ορισμός κλάσης Jar (βασική κλάση του προγράμματος)
        job.setJarByClass(Q1.class);
        //Ορισμός κλάσεων Mapper και Reducer
        job.setMapperClass(InvertedIndexMapper.class);
        // *****Θα μπορούσε να υπάρχει και ένας Combiner για διαφορετική υλοποίηση/προσέγγιση για βελτιστοποίηση****
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    
        // Ορισμός διαδρομών εισόδου/εξόδου από τα ορίσματα γραμμής εντολών
        // args[0] = φάκελος εισόδου (π.χ. /user/root/input)
        // args[1] = φάκελος εξόδου (π.χ. /user/root/output_q1)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

