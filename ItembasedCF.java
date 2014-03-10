package h_wc_p;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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

public class ItembasedCF {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    private IntWritable MovId = new IntWritable();
    private Text UidRateings = new Text();
    //private String movie_ratings;
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	
    	String uid = itr.nextToken();
    	String movieid_ratings;
    	MovId.set(Integer.parseInt(itr.nextToken()));
    	movieid_ratings = uid + ' ' + itr.nextToken();
    	UidRateings.set(movieid_ratings);
    	context.write(MovId, UidRateings);
    }
  }
  public static class PairMapper
       extends Mapper<Object, Text, Text, Text>{
	  //private int itemcounter  = 0;
	  private String  pairitems = new String();
	  public void map(Object key, Text value, Context context)
	  throws IOException, InterruptedException {
		  int itemcounter = 0;
		  String itemname;
		  StringTokenizer Strt = new StringTokenizer(value.toString());
		  itemcounter = (Strt.countTokens() - 2) / 2;//the first one and the last one is the moive and ratings and the others are the user and the corresponding rating
		  itemname = Strt.nextToken();//the first one is the commodity name
		  String[] items = new String[itemcounter];//store the items
		  String[] ratings = new String[itemcounter];//store the corresponding ratings
		  if(itemcounter > 1){//those who purchase the item should more than 1;
		  for(int ite = 0;ite < itemcounter;ite++)//the next itemcounter items is the user name;
		  {
			  items[ite] = Strt.nextToken();//copy the user items to the array
		      ratings[ite] = Strt.nextToken();
		  }		  
			  String oppair = new String(itemname + ' ' + Strt.nextToken());//commodity and the corresponding average ratings;
		  int i = 0;
		  int j = 1;
		  for(i = 0;i < itemcounter;i++)
		  {
			  for(j = i + 1;j < itemcounter;j++)
			  {
				  if(Integer.parseInt(items[j]) < Integer.parseInt(items[i]))
					  pairitems = items[j] + ' ' + items[i];
				  else
				  pairitems = items[i] + ' ' + items[j];//user pair
				  //oppair = ratings[i] + ' ' + ratings[j] + ' ' + oppair;
				  context.write(new Text(pairitems), new Text(ratings[i] + ' ' + ratings[j] + ' ' +   oppair));//the key is <user1 ,user2> ,the value is <ratings1,ratings2,moviename,averageratings>;
			  }
		  }
		  }
		  
	  }
  }
  //The CompuateSimilarityMapper is to compute the similarity between two items;
  //I had mistaked Mapper for map fuction,so the error of the mismatch;
  public static class ComputeSMapper
       extends Mapper<Object, Text, Text, Text>{
	   public void map(Object key, Text value, Context context)
	   throws IOException, InterruptedException {
		  StringTokenizer s = new StringTokenizer(value.toString());
		  String record1 = new String();
		  String record2 = new String();
		  String[] set = new String[s.countTokens()];
		  for(int i = 0; i < 10; i ++){
			  set[i] = s.nextToken();
		  }
		  record1 = set[0] + ' ' + set[2] + ' ' + set[4] + ' ' + set[8] + ' ' + set[9];
		  record2 = set[1] + ' ' + set[3] + ' ' + set[5] + ' ' + set[8] + ' ' + set[9];
		  Text t = new Text();
		  if(Integer.parseInt(set[7]) < Integer.parseInt(set[6]))
			  t.set(set[7] + ' ' + set[6]);
		  else
		      t.set(set[6] + ' ' + set[7]);
		  context.write(t, new Text(record1));
		  context.write(t, new Text(record2));		  
		  //context.write(new Text("hello"), new Text("world"));
	  }
  }
  public static class AverageRatingsReducer
       extends Reducer<IntWritable, Text, Text, FloatWritable>{
	  private FloatWritable average_rat = new FloatWritable();
	 // private String s = new String();
	  public void reduce(IntWritable key, Iterable<Text> values,
			             Context context
			             )throws IOException, InterruptedException{
		  float sum = 0.0f;
		  int num = 0;
		 // String s;
		  //StringTokenizer str = new StringTokenizer("nihao");
		  String s = new String();
		  String uid;
		  String ratings = new String();
		  for(Text val : values){
			  StringTokenizer str = new StringTokenizer(val.toString());
			  uid = str.nextToken();
			  //s = s + ' ' + uid;
			  ratings = str.nextToken(); 
			  s = s + ' ' + uid + ' ' + ratings;//userid combine the ratings of the user at the item;
			  sum +=Float.parseFloat(ratings);
			  num ++;
		  }
		 //key = new Text(key.toString() + ' ' + s);
		  average_rat.set(sum / num);
		  context.write(new Text(key.toString() + ' ' + s), average_rat);
		 /* Text a = new Text();
		  for(Text val : values){
			  a.set(val.getBytes());
		  }
		  context.write(key, a); */
	  }
  }
  public static class PairgeneratedReducer
       extends Reducer<Text, Text, Text, Text>{
	  public void reduce(Text key, Iterable<Text> values,
			             Context context
			             )throws IOException, InterruptedException{
		  String ubpairitem = new String();
		  String ubpairratings = new String();
		  String userpairid = new String();
		  String itemspairaddratings = new String();
		  int count = 0;
		   for(Text val : values)//val is ratings of user1 and user 2 and the movieid and corresponding ratings;
		   {
			   ubpairitem = ubpairitem + ' ' + val.toString();
			   count++;
		   }
		   count = count + count + count + count;//
		   String[] itemrecord = new String[count];
		   StringTokenizer s = new StringTokenizer(ubpairitem);
		   for(int i = 0;i < count; i ++){
			   itemrecord[i] = s.nextToken();
		   }
		   if(count > 4)
		     for(int j = 2;j < count;j = j + 4){
			     for(int k = j + 4;k < count; k = k + 4){
				     ubpairitem = itemrecord[j] + ' ' + itemrecord[k]; //the commom items two user buy together;
				     userpairid = itemrecord[j -1] + ' ' + itemrecord[j-2] + ' ' + itemrecord[k - 1] + ' ' + itemrecord[k - 2];//ratings of user1 and user2 at the two movies;
				     ubpairratings = itemrecord[j + 1] + ' '  + itemrecord[k + 1];//the ratings of the corresponding items;
				     itemspairaddratings = ubpairitem + ' ' + ubpairratings;//the items and the average ratings for the output value;
				     context.write(key, new Text(userpairid + ' ' + itemspairaddratings));//the output is user1, user2, u1-m1-ratings, u2
			      }
		      }
		   
	  }
  }
  public static class ComputeSReducer
       extends Reducer< Text, Text, Text, DoubleWritable>{
	  public void reduce(Text key, Iterable<Text> values,
			             Context context
			             )throws IOException, InterruptedException{
		  String[] str = new String[5];
		  Float sub1 = 0.0f;
		  Float sub2 = 0.0f;
		  double add1 = 0.0f;
		  double add2 = 0.0f;
		  //Float mul1 = 0.0f; 
		  //Float mul2 = 0.0f;
		  double sum1 = 0.0f;
		  double sum2 = 0.0f;
		  StringTokenizer s;
		  for(Text val : values){
			  s = new StringTokenizer(val.toString());
			  for(int i = 0; i < 5; i ++)
				  str[i] = s.nextToken();
			  sub1 = Float.  parseFloat(str[1]) - Float.parseFloat(str[3]);
			  sub2 = Float.parseFloat(str[2]) - Float.parseFloat(str[4]);
			  add1 = sub1 * sub1 + add1;
			  add2 = sub2 * sub2 + add2;
			  sum1 = sum1 + sub1 * sub2; 
		  }
		  if( add1 == 0.0f || add2 == 0.0f)
			  context.write(key, new DoubleWritable(0.0));//if the movie is the greatest or the worst,than it will has no similarity with others;
		  else{
		  sum2 = Math.sqrt(add1) * Math.sqrt(add2);
		      if(sum2 == 0.0f)
		    	  context.write(key, new DoubleWritable(0.0));
		      else
		          context.write(key , new DoubleWritable(sum1 / sum2));
		      }
		  /*for(Text val : values){
			  StringTokenizer s = new StringTokenizer(val.toString());
			  context.write(key, new DoubleWritable(s.countTokens()));  
		  } */
	  }
  }
  public static void main(String[] args) throws Exception{
	  Configuration conf = new Configuration();
	  /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  if(otherArgs.length != 2) {
		  System.err.println("Usage: ItembasedCF<in><out>");
		  System.exit(2);
	  }*/
	  Job job1 = new Job(conf, "Item basedCF");
	  job1.setJarByClass(ItembasedCF.class);
	  job1.setMapperClass(TokenizerMapper.class);
	  job1.setReducerClass(AverageRatingsReducer.class);
	  job1.setMapOutputKeyClass(IntWritable.class);
	  job1.setMapOutputValueClass(Text.class);
	  job1.setOutputKeyClass(Text.class);
	  job1.setOutputValueClass(FloatWritable.class);
	  FileInputFormat.addInputPath(job1, new Path("input02/testdata.dat"));
	  FileOutputFormat.setOutputPath(job1, new Path("output10"));
	  //System.exit(job1.waitForCompletion(true) ? 0 : 1); 
	  job1.waitForCompletion(true);
	  
	  Job job2 = new Job(conf, "prepare items similarity");
	  job2.setNumReduceTasks(5);
	  job2.setJarByClass(ItembasedCF.class);
	  job2.setMapperClass(PairMapper.class);
	  job2.setReducerClass(PairgeneratedReducer.class);
	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job2, new Path("output10"));
	  FileOutputFormat.setOutputPath(job2,new Path("output11"));
	  //System.exit(job2.waitForCompletion(true) ? 0 : 1);
	  job2.waitForCompletion(true);
	  
	  Job job3 = new Job(conf, "prepare items similarity");
	  job3.setNumReduceTasks(5);
	  job3.setJarByClass(ItembasedCF.class);
	  job3.setMapperClass(ComputeSMapper.class);
	  job3.setReducerClass(ComputeSReducer.class);
	  job3.setMapOutputKeyClass(Text.class);
	  job3.setMapOutputValueClass(Text.class);
	  job3.setOutputKeyClass(Text.class);
	  job3.setOutputValueClass(DoubleWritable.class);
	  //job3.setInputFormatClass(TextInputFormat.class);
	  FileInputFormat.addInputPath(job3, new Path("output11"));
	  FileOutputFormat.setOutputPath(job3,new Path("output12"));
	  System.exit(job3.waitForCompletion(true) ? 0 : 1);
	    
	   
  }
  
}
