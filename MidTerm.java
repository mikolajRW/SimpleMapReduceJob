import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;


public class MidTerm extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(MidTerm.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MidTerm(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        
        Configuration conf = this.getConf();
        String temporaryPath = conf.get("tmpPath",  "user/bigdata49/tmp18");
        Path tmpPath = new Path(temporaryPath);


        Job job = Job.getInstance(this.getConf(), "Anagram count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(MidTermMap.class);
        job.setReducerClass(MidTermReduce.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, tmpPath);

        job.setJarByClass(MidTerm.class);

        boolean result = job.waitForCompletion(true);
 
        if(result){
            Job jobB = Job.getInstance(conf, "Top 11");
            jobB.setOutputKeyClass(IntWritable.class);
            jobB.setOutputValueClass(Text.class);

            jobB.setMapOutputKeyClass(Text.class);
            jobB.setMapOutputValueClass(IntWritable.class);

            jobB.setMapperClass(Top11MapperGet.class);
            jobB.setReducerClass(Top11ReducerGet.class);
            jobB.setNumReduceTasks(1);
            FileInputFormat.setInputPaths(jobB, tmpPath);
            FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
            jobB.setInputFormatClass(KeyValueTextInputFormat.class);
            jobB.setOutputFormatClass(TextOutputFormat.class);

            jobB.setJarByClass(MidTerm.class);
	        result = jobB.waitForCompletion(true);
        }
        return result ? 0 : 1;
    }



    public static class MidTermMap extends Mapper<Object, Text, Text, Text> {
        List<String> commonWords = Arrays.asList(new String []{"the", "a", "an", "and", "of", 
        "to", "in", "am", "is", "are", "at", "not"});
        List<String> stopWords;
        String delimiters;
        

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String wordDelimiters = " \t,;.?!-:@[](){}_*/'";
            StringTokenizer tokenizer = new StringTokenizer(line, wordDelimiters);
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                if (!nextToken.isEmpty() && !commonWords.contains(nextToken.trim())) {
                    char [] chars = nextToken.trim().toCharArray();
                    Arrays.sort(chars);
                    String sorted_string = new String(chars);
                    String val = nextToken.trim();
                    context.write(new Text(sorted_string), new Text(val));
               
            }
        }
    }}

    public static class MidTermReduce extends Reducer<Text, Text, IntWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int occur = 0;
            HashSet <String> uniqueWords = new HashSet<>();
            for (Text val : values) {
                String word = new String(val.toString());
                uniqueWords.add(word);
                occur++;
            }
            String result = String.valueOf(occur);
            String toLex [] = new String[uniqueWords.size()];
            int i=0;
            for(String word:uniqueWords){
                toLex[i++] = word;
            }
            Arrays.sort(toLex);
            for(i=0; i <uniqueWords.size();i++){
                result += " " + toLex[i];
            }
            String finalKey = String.valueOf(uniqueWords.size());
            
            context.write(new IntWritable(uniqueWords.size()), new Text(result));
            
           
        }
    }

    static int SIZE = 11;
    public static class Top11MapperGet extends Mapper<Text, Text, Text, IntWritable> {
        private TreeSet<ComparablePair<Integer, String>> countTop11 = new TreeSet<ComparablePair<Integer, String>>();
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int getKey = Integer.parseInt(key.toString());
            String val = value.toString();
            
            countTop11.add(new ComparablePair<Integer,String>(getKey, val));

            if(countTop11.size() > SIZE){
                countTop11.pollFirst();
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        
            for (ComparablePair<Integer, String> pair : countTop11) {
                context.write(new Text(pair.getValue()), new IntWritable(pair.getKey()));
            }
        }
    }

    public static class Top11ReducerGet extends Reducer<Text, IntWritable, IntWritable, Text> {
        
        private TreeSet<ComparablePair<Integer, String>> countTop11 = new TreeSet<ComparablePair<Integer, String>>();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyVal = key.toString();
            int count = 0;
            for(IntWritable val:values){
                count = val.get();
            }
            countTop11.add(new ComparablePair<Integer,String>(count, keyVal));
            if(countTop11.size()>SIZE){
                countTop11.pollFirst();
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (ComparablePair<Integer, String> pair : countTop11) {
                    context.write(new IntWritable(pair.getKey()), new Text(pair.getValue()));
            }
        }
    }

    static class ComparablePair<K extends Comparable<? super K>, V extends Comparable<? super V>>
    extends javafx.util.Pair<K, V>
    implements Comparable<ComparablePair<K, V>> {

    public ComparablePair(K key, V value) {
        super(key, value);
    }

    @Override
    public int compareTo(ComparablePair<K, V> o) {
        int cmp = o == null ? -1 : (this.getKey()).compareTo(o.getKey()); 
        return cmp == 0 ? (this.getValue()).compareTo(o.getValue()) : cmp;
        }
    }

}
