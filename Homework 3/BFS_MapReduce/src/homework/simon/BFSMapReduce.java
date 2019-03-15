package homework.simon;
/*
*
* ECE 6130 Big Data and Cloud Computing
* Spring 2019
* Homework 3: BFS using MapReduce (Hadoop)
* Name: Tianyu Yang
* GW ID:G38878678
* Referenced from https://puffsun.iteye.com/blog/1905524
*
*/
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;  
  
import java.io.BufferedReader;  
import java.io.IOException;  
import java.io.InputStreamReader;  
import java.util.HashMap;  
  
public class BFSMapReduce extends Configured implements Tool {  
  
    public static String OUT = "output";  
    public static String IN = "inputlarger";  
  
    public static class DijkstraMapper extends Mapper<LongWritable, Text, LongWritable, Text> {  
  
        public void map(LongWritable key, Text value, Context context)  
                throws IOException, InterruptedException {  
  
            //From slide 20 of Graph Algorithms with MapReduce (by Jimmy Lin, Univ @ Maryland)  
            //Key is node n  
            //Value is D, Points-To  
            //For every point (or key), look at everything it points to.  
            //Emit or write to the points to variable with the current distance + 1  
            Text word = new Text();  
            String line = value.toString();//looks like 1 0 2:3:  
            String[] sp = line.split(" ");//splits on space  
            int distanceAdded = Integer.parseInt(sp[1]) + 1;  
            String[] pointsTo = sp[2].split(":");  
            for (String distance : pointsTo) {  
                word.set("VALUE " + distanceAdded);//tells me to look at distance value  
                context.write(new LongWritable(Integer.parseInt(distance)), word);  
                word.clear();  
            }  
            //pass in current node's distance (if it is the lowest distance)  
            word.set("VALUE " + sp[1]);  
            context.write(new LongWritable(Integer.parseInt(sp[0])), word);  
            word.clear();  
  
            word.set("NODES " + sp[2]);//tells me to append on the final tally  
            context.write(new LongWritable(Integer.parseInt(sp[0])), word);  
            word.clear();  
  
        }  
    }  
  
    public static class DijkstraReducer extends Reducer<LongWritable, Text, LongWritable, Text> {  
        public void reduce(LongWritable key, Iterable<Text> values, Context context)  
                throws IOException, InterruptedException {  
  
            //From slide 20 of Graph Algorithms with MapReduce (by Jimmy Lin, Univ @ Maryland)  
            //The key is the current point  
            //The values are all the possible distances to this point  
            //we simply emit the point and the minimum distance value  
  
            String nodes = "UNMODED";  
            Text word = new Text();  
            int lowest = 10009;//start at infinity  
  
            for (Text val : values) {//looks like NODES/VALUES 1 0 2:3:, we need to use the first as a key  
                String[] sp = val.toString().split(" ");//splits on space  
                //look at first value  
                if (sp[0].equalsIgnoreCase("NODES")) {  
                    nodes = null;  
                    nodes = sp[1];  
                } else if (sp[0].equalsIgnoreCase("VALUE")) {  
                    int distance = Integer.parseInt(sp[1]);  
                    lowest = Math.min(distance, lowest);  
                }  
            }  
            word.set(lowest + " " + nodes);  
            context.write(key, word);  
            word.clear();  
        }  
    }  
  
    //Almost exactly from http://hadoop.apache.org/mapreduce/docs/current/mapred_tutorial.html  
    public int run(String[] args) throws Exception {  
        //http://code.google.com/p/joycrawler/source/browse/NetflixChallenge/src/org/niubility/learning/knn/KNNDriver.java?r=242  
        //make the key -> value space separated (for iterations)  
        getConf().set("mapred.textoutputformat.separator", " ");  
  
        //set in and out to args.  
        IN = args[0];  
        OUT = args[1];  
  
        String infile = IN;  
        String outputfile = OUT + System.nanoTime();  
  
        boolean isdone = false;  
        boolean success = false;  
  
        HashMap<Integer, Integer> _map = new HashMap<Integer, Integer>();  
  
        while (!isdone) {  
  
            Job job = new Job(getConf(), "Dijkstra");  
            job.setJarByClass(ParallelDijkstra.class);  
            job.setOutputKeyClass(LongWritable.class);  
            job.setOutputValueClass(Text.class);  
            job.setMapperClass(DijkstraMapper.class);  
            job.setReducerClass(DijkstraReducer.class);  
            job.setInputFormatClass(TextInputFormat.class);  
            job.setOutputFormatClass(TextOutputFormat.class);  
  
            FileInputFormat.addInputPath(job, new Path(infile));  
            FileOutputFormat.setOutputPath(job, new Path(outputfile));  
  
            success = job.waitForCompletion(true);  
  
            //remove the input file  
            //http://eclipse.sys-con.com/node/1287801/mobile  
            if (!infile.equals(IN)) {  
                String indir = infile.replace("part-r-00000", "");  
                Path ddir = new Path(indir);  
                FileSystem dfs = FileSystem.get(getConf());  
                dfs.delete(ddir, true);  
            }  
  
            infile = outputfile + "/part-r-00000";  
            outputfile = OUT + System.nanoTime();  
  
            //do we need to re-run the job with the new input file??  
            //http://www.hadoop-blog.com/2010/11/how-to-read-file-from-hdfs-in-hadoop.html  
            isdone = true;//set the job to NOT run again!  
            Path ofile = new Path(infile);  
            FileSystem fs = FileSystem.get(new Configuration());  
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));  
  
            HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();  
            String line = br.readLine();  
            while (line != null) {  
                //each line looks like 0 1 2:3:  
                //we need to verify node -> distance doesn't change  
                String[] sp = line.split(" ");  
                int node = Integer.parseInt(sp[0]);  
                int distance = Integer.parseInt(sp[1]);  
                imap.put(node, distance);  
                line = br.readLine();  
            }  
            if (_map.isEmpty()) {  
                //first iteration... must do a second iteration regardless!  
                isdone = false;  
            } else {  
                //http://www.java-examples.com/iterate-through-values-java-hashmap-example  
                //http://www.javabeat.net/articles/33-generics-in-java-50-1.html  
                for (Integer key : imap.keySet()) {  
                    int val = imap.get(key);  
                    if (_map.get(key) != val) {  
                        //values aren't the same... we aren't at convergence yet  
                        isdone = false;  
                    }  
                }  
            }  
            if (!isdone) {  
                _map.putAll(imap);//copy imap to _map for the next iteration (if required)  
            }  
        }  
  
        return success ? 0 : 1;  
    }  
  
    public static void main(String[] args) throws Exception {  
        System.exit(ToolRunner.run(new ParallelDijkstra(), args));  
    }  
} 

