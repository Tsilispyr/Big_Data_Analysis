//Διαχείριση Δεδομένων Μεγάλου Όγκου 2025-2026
//Αρχείο Q2
//Εκφώνηση
// (Επέκταση Ανεστραμμένου ευρετηρίου Q1) Επεκτείνετε το ανεστραμμένο
// ευρετήριο που φτιάξατε στο 1ο Ερώτημα, ώστε, εκτός από το όνομα του αρχείου στο οποίο εμφανίζεται
// η κάθε λέξη, να καταγράφεται και η συχνότητα εμφάνισής της στο κάθε αρχείο. Καταγράψτε αυτή την
// πληροφορία μόνο για λέξεις με 3 ή περισσότερους χαρακτήρες. Για παράδειγμα, αν η λέξη “the”
// εμφανίζεται 50 φορές στο pg100, 20 φορές στο el_quijote και 25 φορές στο pg46, η λέξη “thou”
// εμφανίζεται 37 φορές στο pg100.txt, ενώ η λέξη “an” εμφανίζεται 200 φορές στο pg100, 100 φορές στο
// el_quijote και 50 φορές στο pg46, το αντίστοιχο output του προγράμματός σας θα πρέπει να είναι:
// the pg100:50, el_quijote:20, pg46:25
// thou pg100:37

// Υπάρχουν αναλυτικά σχόλια για την αντικατάσταση αχείου Readme


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

public class Q2 {
/*
Κλάση Mapper: InvertedIndexMapper
Διαβάζει το αρχείο εισόδου γραμμή προς γραμμή.
Είσοδος (Input):
-Key: Object (Offset γραμμής).
-Value: Text (Περιεχόμενο γραμμής).
Για *κάθε γραμμή*, μετράει τοπικά τη συχνότητα των λέξεων (με >= 3 chars).
Αυτή η τεχνική (in-mapper combining) μειώνει τον όγκο των ενδιάμεσων
δεδομένων που στέλνονται στον Reducer.
Έξοδος (Output) - Ενδιάμεσα ζεύγη (key, value):
-Key: Text (Η λέξη, π.χ. "the").
-Value: Text (Το όνομα του αρχείου και η τοπική συχνότητα, 
π.χ. "pg100.txt:5").
Αν η λέξη "the" εμφανιστεί 5 φορές σε μια γραμμή, ο Mapper θα στείλει 
ένα ζεύγος ("the", "pg100.txt:5") αντί για 5 ζεύγη ("the", "pg100.txt").
*/
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\W+");
        // Επαναχρησιμοποιούμενα αντικείμενα Text
        private Text word = new Text();
        private Text fileAndCount = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            //λήψη ονόματος αρχείου
            String line = value.toString().toLowerCase();
            //Επεξεργασία γραμμής
            String[] tokens = WORD_BOUNDARY.split(line);

            //Μέτρηση συχνότητας των λέξεων στη γραμμή με ενα Hashmap
            Map<String, Integer> localCounts = new HashMap<>();
            for (String token : tokens) {
                if (token.length() >= 3) { // μόνο λέξεις με 3+ χαρακτήρες
                    localCounts.put(token, localCounts.getOrDefault(token, 0) + 1);
                }
            }

            //(λέξη, "filename:count") για κάθε λέξη
            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                word.set(entry.getKey());
                fileAndCount.set(fileName + ":" + entry.getValue());
                context.write(word, fileAndCount);
            }
        }
    }

/*
Κλάση Reducer: InvertedIndexReducer
Λαμβάνει τα ομαδοποιημένα δεδομένα από τους Mappers.
--Είσοδος (Input):
 -Key: Text (Μια μοναδική λέξη, π.χ. "the").
 -Values: Iterable<Text> (Μια λίστα με *όλες* τις τιμές "αρχείο:μέτρηση" 
από όλους τους mappers, π.χ. ["pg100.txt:5", "pg100.txt:3", "el_quijote:10"]).
της λέξης σε κάθε αρχείο.
--Έξοδος (Output) - Τελικό αποτέλεσμα:
 -Key: Text (Η λέξη, π.χ. "the").
 -Value: Text (Η τελική λίστα αρχείων και συχνοτήτων, 
π.χ. "pg100:8, el_quijote:10").
*/


    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //To σύνολο συχνοτήτων μπαίνει σε ανά αρχείο, με HashMap από mappers για κάθε αρχείο
            Map<String, Integer> fileCounts = new HashMap<>();

            for (Text val : values) {
                //Parsing της τιμής (π.χ. "pg100.txt:5")
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String fileName = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    fileCounts.put(fileName, fileCounts.getOrDefault(fileName, 0) + count);
                }
            }

            // Τελική γραμμή / Δημιουργία τελικού string εξόδου
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : fileCounts.entrySet()) {
                if (sb.length() > 0) sb.append(", "); //διαχωριστικό
                sb.append(entry.getKey()).append(":").append(entry.getValue());
            }
            //τελικό αποτέλεσμα
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    //Κλάση main
    //Ρυθμίζει και ξεκινάει το MapReduce.

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index with Frequency");
        job.setJarByClass(Q2.class);
        //Ορισμός κλάσεων Jar, Mapper, Reducer
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        //Ορισμός τύπων εξόδου (Reducer)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //Ορισμός διαδρομών εισόδου/εξόδου
        //args[0] = φάκελος εισόδου (π.χ. /user/root/input)
        //args[1] = φάκελος εξόδου (π.χ. /user/root/output_q2)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
