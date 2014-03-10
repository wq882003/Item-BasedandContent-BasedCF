package h_wc_p;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

interface CalInterface {
	int a = 0;
	int getAera(float r);
}

abstract class Graphic {
	int a;
	abstract void close();
	abstract void open();
}

class Point<T> {
	private T var;
	public void setVar(T var){
		this.var = var;
	}
}

public class ModifiedMyPrediction extends Configured implements Tool {
	public static class Predictionmapper 
	     extends Mapper<Object, Text, IntWritable, Text>{
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			//String str1 = new String(value.toString());
			//String[] s = str1.split("	| ");
		    //int uid = Integer.parseInt(s[0]);
		   // int mid = Integer.parseInt(s[1]);
	        //float rating = Float.parseFloat(s[2]);
		    StringTokenizer s = new StringTokenizer(value.toString());
		    //int uid = s.nextToken();
		     
		    	
			context.write(new IntWritable(Integer.parseInt(s.nextToken())), new Text(s.nextToken() + ' ' + s.nextToken()));
			
		}
	}
	public static class MyPartitionerPar extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartition) {
			int result = 0;
			return result;
		}
	}
	public static class Predictiongeneratemapper
	     extends Mapper<Object, Text, IntWritable, Text> {
		String []s = null;
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
			//String s1 = "userrecord.dat";
			//String s2 = "itemsaverating.dat";
			//BufferedReader reader1 = new BufferedReader(new FileReader(s1));
			//BufferedReader reader2 = new BufferedReader(new FileReader(s2));
			s = value.toString().split(" |::|	");
			 if (s.length == 4) {
				 context.write(new IntWritable(Integer.parseInt(s[0])), new Text(s[1] + ' ' + s[2] + ' ' + s[3]));
				 context.write(new IntWritable(Integer.parseInt(s[1])), new Text(s[0] + ' ' + s[2] + ' ' + s[3]));
			 }else if(s.length == 3){
				 context.write(new IntWritable(Integer.parseInt(s[0])), new Text(s[1] + ' ' + s[2]));
				 context.write(new IntWritable(Integer.parseInt(s[1])), new Text(s[0] + ' ' + s[2]));
			 } else {
				 throw new IOException("the data format is error!");
			 }
		}
	}
	public static class Predictionreducer
	     extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> value, Context context)
		throws IOException, InterruptedException{
			Text t = new Text();
			String s = new String();
			for(Text val : value)
				s = s + ' ' + val;
			t.set(s);
			context.write(key, t);
				
		}
	}
	public static class PredictionGenereducer
	     extends Reducer<IntWritable, Text, Text, FloatWritable>{
		BufferedReader reader1 = null;//user items and ratings
		BufferedReader reader2 = null;//ave ratings
		HashMap<Integer, Float> map1 = new HashMap<Integer, Float>();//average ratings
		@Override
		protected void setup(Context context)//setup is a funtion which initiated the reduce task
	    throws IOException, InterruptedException{
		    super.setup(context);
		   /* Path[] localcachefiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		    if(localcachefiles != null)
		    {
		    	for(int i = 0;i < localcachefiles.length;i ++)
		    	{
		    		
		    	}
		    }
		    reader1 = new BufferedReader(new FileReader(new File("d1")));
			reader2 = new BufferedReader(new FileReader(new File("d2")));*/
		    String str2;
			BufferedReader reader2 = new BufferedReader(new FileReader(new File("d2")));
			while((str2 = reader2.readLine()) != null){
				//System.out.println(str2);
				String[] ss = str2.split(" |	"); 
				map1.put(Integer.parseInt(ss[0]), Float.parseFloat(ss[ss.length - 1]));
				//context.write(new Text(ss[0]), new FloatWritable(Float.parseFloat(ss[ss.length - 1])));////////
			}
			reader2.close(); 
		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			HashMap<Integer, Float> map2 = new HashMap<Integer, Float>();//attribuates similarity
			String allvalues = new String(); 
			String  [] s = null;
			System.out.println("stage 1");
			for(Text val : values)
			{
				s = val.toString().split(" ");
				if(s.length == 2)
					allvalues = allvalues + val.toString() + ' ';
				else if(s.length == 3){
					int itemid = Integer.parseInt(s[0]);
					float attrsimi = Float.parseFloat(s[1]);
					if(!map2.containsKey(itemid)){
						map2.put(itemid, attrsimi);
					}
				/*	else {
						throw new IOException("data error" + ' ' + itemid + ' ' + map2.get(itemid) + val.toString());
					}*/
				}
				else {
					throw new IOException("data error");
				}
			}
			System.out.println("stage 2");
			String []itemrating = allvalues.split(" ");
			/*String s1 = "userrecord.data";
			String s2 = "itemsaverating.data";*/
			BufferedReader reader1 = new BufferedReader(new FileReader("d1"));
		
			String str1;
			/*String str2;
			HashMap<Integer, Float> map1 = new HashMap<Integer, Float>();//average ratings
			while((str2 = reader2.readLine()) != null){
				//System.out.println(str2);
				String[] ss = str2.split(" |	"); 
				map1.put(Integer.parseInt(ss[0]), Float.parseFloat(ss[ss.length - 1]));
				//context.write(new Text(ss[0]), new FloatWritable(Float.parseFloat(ss[ss.length - 1])));////////
			}
			reader2.close();*/
			while((str1 = reader1.readLine()) != null){
				StringTokenizer st = new StringTokenizer(str1.toString());
				String uid = st.nextToken();
				HashMap<Integer, Float> map = new HashMap<Integer, Float>();//the items of user i 
				while(st.hasMoreTokens()){
					int item = Integer.parseInt(st.nextToken());
					float rating = Float.parseFloat(st.nextToken());
					if(!map.containsKey(item))
						map.put(item, rating);	
				    //context.write(new Text("123"), new FloatWritable(1.0f));//////
				}
				float ssum = 0.0f;//fenmu de sum
				float multisum = 0.0f;//fenzi de sum
				float tmpsum = 0.0f;
				float tmpmultisum = 0.0f;
				float lamda = 0.8f;
				float eta = 0.8f;
				if(!map.containsKey(key.get())){//if user i do not have the item key, then predict;
				    //for(Text val : values){
			    	int len = itemrating.length - 1;
			    	boolean tag = false;
			    	boolean tag1 = false;
			    	for(int i = 0;i < len;i = i + 2){
					    //String[] itemandsimi = val.toString().split(" ");
			    		int itemid = Integer.parseInt(itemrating[i]);
			    		float simi = Float.parseFloat(itemrating[i + 1]);
			    		float rating = 0.0f;
				    	float averating = 0.0f;
					    if(map.containsKey(itemid) && map1.containsKey(itemid) && Math.abs(simi) > 0.5f){// if user has the item [0], and the similarity is more than 0.5f, then compute the prediction
					    	tag1 = true;
					    	rating = map.get(itemid);
					    	averating = map1.get(itemid);
					    	if(Math.abs(simi) > lamda) {
					    		if (tag == false) {
					    			multisum = 0.0f;
					    			ssum = 0.0f;
					    			tag = true;
					    		}
					    		multisum = multisum + simi * (rating -averating);
					    		ssum = ssum + Math.abs(simi);
					    	}
					    	else if(tag == false){
					    		float attrsimi = 0.0f;
					    		if(map2.containsKey(itemid)) {
					    			attrsimi = map2.get(itemid);
					    			if(attrsimi > eta){//if the attrsimi is > 0,8, then...
					    				simi = simi + attrsimi -  2 * (float)((simi * attrsimi)/(simi + map2.get(itemid)));
					    				multisum = multisum + simi * (rating -averating);
					    				ssum = ssum + Math.abs(simi);
					    			}
					    			//else nothing will be done.
					    		}
					    	}//if 0.8>Math.abs(simi)>0.5 and tag == true, then nothing will do
					    }//right here, itemrating[i] should be itemrating[i+1]**************
					    else if(map2.containsKey(itemid)) {
					    	simi = map2.get(itemid);
					    	if(simi >= eta && map.containsKey(itemid) && map1.containsKey(itemid)) {
					    		rating = map.get(itemid);
					    		averating = map1.get(itemid);
					    		tmpmultisum = tmpmultisum + (float)(simi * (rating -averating));
					    		try{
					    			tmpsum += Math.abs(simi);
					    		}catch( NullPointerException e) {
					    			System.out.println("error" + tmpsum);
					    		}
					    	}
					    		
					    }
			    	}
			    	if(tag1 == false) {
			    		multisum = tmpmultisum;
			    		ssum = tmpsum;
			    	}
			    	if(ssum != 0.0f){
			    		float r = 2.5f;
			    		if(map1.containsKey(key.get())){
			    			r = map1.get(key.get());
			    		}
			    		float p = r + multisum / ssum;
			    		context.write(new Text(uid + ' ' + key.toString()), new FloatWritable(p));
			    	} 
			    	/*else 
			    		context.write(new Text(uid + ' ' + key.toString()), new FloatWritable(0.0f));
			    	// the default value should be a research aera, so, i can not specify it by myself; 
			    	*/
			    }
			    //context.write(new Text("j"), new FloatWritable(1.1f));
			}
			reader1.close();
					
			//context.write(new Text("j"), new FloatWritable(1.0f));
		}
	}
	public int run(String[] args) throws IOException {
		/*Configuration conf = new Configuration();
		Job job = new Job(conf, " predictioncf");
		job.setJarByClass(PredictionCF.class);
		job.setMapperClass(Predictionmapper.class);
		job.setReducerClass(Predictionreducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("input02/u.data"));
		FileOutputFormat.setOutputPath(job, new Path("output15"));
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 
		}*/
		Configuration conf1 = new Configuration();
		//conf1.setFloat("lamda", Integer.parseInt(args[0]));
		conf1.set("mapred.task.timeout", "0");
		//conf1.set("mapred.create.symlink", "yes");
		DistributedCache.createSymlink(conf1);
		try {
			//DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/MoviePair8/part-r-00000#d1"), conf1);
			//DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/AverageRatings8/part-r-00000#d2"), conf1);
			DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/output/TestMovieSet/part-r-00000#d1"), conf1);
			DistributedCache.addCacheFile(new URI("hdfs://jsjcloudmaster:9160/user/yc/output/AverageRatings1/part-r-00000#d2"), conf1);
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//conf1.set("mapred.cache.files", "inputdata/userrecord.dat#d1");
		//conf1.set("mapred.cache.files", "inputdata/itemsaverating.dat#d2");
		Job job1 = new Job(conf1, "predicgenecf");
		job1.setJarByClass(ModifiedMyPrediction.class);
		job1.setMapperClass(Predictiongeneratemapper.class);
		job1.setReducerClass(PredictionGenereducer.class);
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		job1.setNumReduceTasks(15);
		FileInputFormat.addInputPath(job1, new Path("hdfs://jsjcloudmaster:9160/user/yc/output/mysimilarity"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://jsjcloudmaster:9160/user/yc/output/Predition2"));
		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public static void main(String[] args) throws Exception{
		  int exitCode = ToolRunner.run(new ModifiedMyPrediction(), args);
				  System.exit(exitCode);
	}
} 