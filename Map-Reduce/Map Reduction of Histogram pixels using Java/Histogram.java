import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable {
    public int type;       /* red=1, green=2, blue=3 */
    public int intensity;  /* between 0 and 255 */
	
	Color(){}
	Color(int t, int in){
		this.type = t;
		this.intensity = in;
		}
		
		
	public void write(DataOutput out) throws IOException {
         out.writeInt(type);
         out.writeInt(intensity);
		 }
       
    public void readFields(DataInput in) throws IOException {
		type = in.readInt();
        intensity = in.readInt();
    }
	
	public String toString() {
        return this.type + " " + this.intensity + " ";
    }
	
    // To sort and order based on reduction	
	public int compareTo(Object o) {
		Color c = (Color)o;
		if(this.type < c.type){
			return -1;
		}
		if(this.type>c.type){
			return 1;
		}
		if(this.intensity > c.intensity){
			    return 1;
		}
		if(this.intensity < c.intensity){
			    return -1;  
		}
		else{
	             return 0;		
			}
		
	}
			
		
	}
	
	
	
    /* need class constructors, toString, write, readFields, and compareTo methods */



public class Histogram {
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
			//Scanning input file for dataset
			Scanner myscan = new Scanner(value.toString()).useDelimiter(",");
			int r = myscan.nextInt();
		//	System.out.println("Prringtin r....."+r);
			int g = myscan.nextInt();
			int b = myscan.nextInt();
			//read 3 numbers from the line and store them in the variables red, green, and blue. Each number is between 0 and 255.
			context.write(new Color(1,r),new IntWritable(r)); 
			context.write(new Color(2,g),new IntWritable(g));
			context.write(new Color(3,b),new IntWritable(b));
			myscan.close();            
        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
		//Reducing based on key passed (r,g,b) and getting count of each
        public void reduce ( Color key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
			long sum = 0;
			for(IntWritable v : values){	
				if (key.intensity == v.get()){
					sum++;
				}
			}
			
			System.out.println("Pringting key number"+key);
			context.write(key,new LongWritable(sum));							
        }
    }

    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Histogram.class);
        job.setOutputKeyClass(Color.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(HistogramMapper.class);
        job.setReducerClass(HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		//input path
        FileInputFormat.setInputPaths(job,new Path(args[0]));
		//output path
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
		
		
   }
}
