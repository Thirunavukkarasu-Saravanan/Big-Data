import java.io.*;
import java.net.URI;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Color implements WritableComparable {

        public int type; 		/* red=1, green=2, blue=3 */
        public int intensity; /* between 0 and 255 */

        Color() {}
        Color(int t, int in )   {
                this.type = t;
                this.intensity = in;
        }

        public void write(DataOutput out) throws IOException {
                out.writeInt(type);
                out.writeInt(intensity);
        }

        public void readFields(DataInput in ) throws IOException {
                type = in.readInt();
                intensity = in.readInt();
        }

		// To sort and order based on reduction	
        public String toString() {
                return this.type + "," + this.intensity + " ";
        }

        public int compareTo(Object o) {
                Color c = (Color)o;
                if(this.type < c.type){ return -1; }
                if(this.type>c.type){ return 1; }
                if(this.intensity > c.intensity){ return 1; }
                if(this.intensity < c.intensity){ return -1; }
                else{ return 0; }
        }
        
        @Override
        public int hashCode() {
                return ((this.type * 1000) + this.intensity);
        }
}

public class Histogram {

        public static Vector<Color> colorLst = new Vector<Color>();
        public static Hashtable<Color,Integer> htColour = new Hashtable<Color,Integer>();

        public static class HistogramMapper extends Mapper < Object, Text, Color, IntWritable > {

                @Override
                public void setup(Context context) throws IOException, NullPointerException {
                        String line;
                        URI[] paths = context.getCacheFiles();
                        Configuration conf = context.getConfiguration();
                        FileSystem fs = FileSystem.get(conf);
                        //Scanning input file for dataset
                Scanner sc = new Scanner(fs.open(new Path(paths[0])));
                while (sc.hasNextLine()) {
                                line = sc.nextLine();
                                String[] rbgColours = line.split(",");
								//read 3 numbers from the line and store them in the variables red, green, and blue. Each number is between 0 and 255.
                                colorLst.add(new Color(1,Integer.parseInt(rbgColours[0])));
                                colorLst.add(new Color(2,Integer.parseInt(rbgColours[1])));
                                colorLst.add(new Color(3,Integer.parseInt(rbgColours[2])));
                        }
                }

                @Override
                public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
                        Enumeration<Color> colorKeys;
                        Color tColor = new Color();
                        colorKeys = htColour.keys();
                        for (Color c: colorLst) {
                                if (htColour.containsKey(c)) {
                                        htColour.put(c, 1);
                                } else {
                                        Integer t = htColour.get(c);
                                        if (t == null){ t = 0;}
                                        t = t + 1;
                                        htColour.put(c, t);
                                }
                        }
                }
                
                @Override
                protected void cleanup(Context context) throws IOException,InterruptedException,NullPointerException {
                        Enumeration<Color> colorKeys;
                        Color key = new Color();
                        colorKeys = htColour.keys();
                        while (colorKeys.hasMoreElements()) {
                                key = (Color) colorKeys.nextElement();
                                context.write(key, new IntWritable(htColour.get(key)));
                        }
                }
        }

        public static class HistogramReducer extends Reducer < Color, IntWritable, Color, LongWritable > {

                @Override
                public void reduce(Color key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
                        long sum = 0;
                        Iterable<IntWritable> values2 = values;
                        for (IntWritable v: values) {
                                sum += v.get();
                        };

                        context.write(key, new LongWritable(sum));
                }
        }

        public static class HistogramCombiner extends Reducer < Color, IntWritable, Color, LongWritable > {

                @Override
                public void reduce(Color key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
                        long sum = 0;
                        for (IntWritable v: values) {
                                sum += v.get();
                        };
                        context.write(key, new LongWritable(sum));
                }
        }
        public static void main(String[] args) throws Exception {
                Job job = Job.getInstance();
                job.setJobName("MyJob");
                job.setJarByClass(Histogram.class);
                job.addCacheFile(new URI(args[0]));

                job.setOutputKeyClass(Color.class);
                job.setOutputValueClass(IntWritable.class);
                job.setMapOutputKeyClass(Color.class);
                job.setMapOutputValueClass(IntWritable.class);
                job.setMapperClass(HistogramMapper.class);
                job.setReducerClass(HistogramReducer.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.setInputPaths(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);


                Job job2 = Job.getInstance();
                job2.setJobName("MyJob2");
                job2.setJarByClass(Histogram.class);
                job2.addCacheFile(new URI(args[0]));
                job2.setOutputKeyClass(Color.class);
                job2.setOutputValueClass(IntWritable.class);
                job2.setMapOutputKeyClass(Color.class);
                job2.setMapOutputValueClass(IntWritable.class);
                job2.setMapperClass(HistogramMapper.class);
                job2.setReducerClass(HistogramReducer.class);
                job2.setCombinerClass(HistogramCombiner.class);
                job2.setInputFormatClass(TextInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);
                FileInputFormat.setInputPaths(job2, new Path(args[0]));
                String output2 = args[1] + "2";
                FileOutputFormat.setOutputPath(job2, new Path(output2));
                job2.waitForCompletion(true);

        }
}
