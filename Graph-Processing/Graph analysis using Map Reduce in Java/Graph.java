import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable {
	public short tag; // 0 for a graph vertex, 1 for a group number
	public long group; // the group where this vertex belongs to
	public long VID; // the vertex ID
	public Vector<Long> adjacent; // the vertex neighbors
	/* ... */

	Vertex() {
		constructorFn((short) 0, 0, 0, null);
	}

	Vertex(short tag1, long group1, long VID1, Vector<Long> adjacent1) {
		constructorFn(tag1, group1, VID1, adjacent1);
	}

	Vertex(short tag1, Long group1) {
		constructorFn(tag1, group1, 0, null);
	}

	private void constructorFn(short t, long g, long v, Vector<Long> a) {
		tag = t;
		group = g;
		VID = v;
		if (a == null)
			adjacent = new Vector<Long>();
		else
			adjacent = a;
	}

	public void write(DataOutput out) throws IOException {
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(VID);

		LongWritable s = new LongWritable(adjacent.size());
		s.write(out);
		for (Long v : adjacent) {
			out.writeLong(v);
		}
	}

	public void readFields(DataInput in) throws IOException {
		tag = in.readShort();
		group = in.readLong();
		VID = in.readLong();
		// read vector

		adjacent = new Vector<Long>();
		LongWritable size = new LongWritable();

		// LongWritable vnbr = new Longwritable();
		// readFields(DataInput in)
		size.readFields(in);
		for (int i = 0; i < size.get(); i++) {
			// add h
			LongWritable desadj = new LongWritable();
			desadj.readFields(in);
			adjacent.add(desadj.get());
		}
	}
}

public class Graph {

	/* ... */

	public static class FirstMapper extends Mapper<Object, Text, LongWritable, Vertex> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner myscan = new Scanner(value.toString()).useDelimiter(",");
			long VID = myscan.nextLong();
			Vector<Long> adjacent = new Vector<Long>();
			while (myscan.hasNext()) {
				long edgadj = myscan.nextLong();
				adjacent.add(edgadj);
			}
			short tag = 0;
			context.write(new LongWritable(VID), new Vertex(tag, VID, VID, adjacent));
		}
	}

	public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
		public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException {

			// pass the graph topology
			context.write(new LongWritable(vertex.VID), vertex);
			short tag = 1;
			for (Long n : vertex.adjacent) {
				// send the group # to the adjacent vertices
				context.write(new LongWritable(n), new Vertex(tag, vertex.group));
			}
		}
	}

	public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
		public void reduce(LongWritable vid, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {
			long m = Long.MAX_VALUE;
			Vector<Long> adj = new Vector<Long>();
			for (Vertex v : values) {
				if (v.tag == 0) {
					adj = (Vector) v.adjacent.clone();
				}

				m = Math.min(m, v.group);
			}

			short tag = 0;
			// new group #
			context.write(new LongWritable(m), new Vertex(tag, m, (Long) vid.get(), adj));
		}
	}

	public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
		@Override
		public void map(LongWritable group, Vertex value, Context context) throws IOException, InterruptedException {
			context.write(group, new LongWritable(1));
		}
	}

	public static class FinalReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		@Override
		public void reduce(LongWritable group, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long m = 0;
			for (LongWritable v : values) {
				m = m + v.get();
			}
			context.write(group, new LongWritable(m));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("MyJob");
		/* ... First Map-Reduce job to read the graph */
		job.setJarByClass(Graph.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		job.setMapperClass(FirstMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//ip
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//op
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/f0"));
		job.waitForCompletion(true);

		for (short i = 0; i < 5; i++) {
			job = Job.getInstance();
			/* ... Second Map-Reduce job to propagate the group number */
			job.setJobName("MyJob2");
			job.setJarByClass(Graph.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Vertex.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Vertex.class);
			job.setMapperClass(SecondMapper.class);
			job.setReducerClass(SecondReducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			//ip
			FileInputFormat.setInputPaths(job, new Path(args[1]+"/f"+i));
			//op
			FileOutputFormat.setOutputPath(job, new Path(args[1]+"/f"+(i+1)));
			job.waitForCompletion(true);
		}

		job = Job.getInstance();
		/* ... Final Map-Reduce job to calculate the connected component sizes */
		job.setJobName("MyJob3");
		job.setJarByClass(Graph.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setMapperClass(FinalMapper.class);
		job.setReducerClass(FinalReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//ip
		FileInputFormat.setInputPaths(job, new Path(args[1]+"/f5"));
		//op
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

	}
}
