package my;
        
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Tweet_join 
{
	
	
	
	// Mapper for Mapping out the key-Value of 'tweets.csv'
        
	public static class TweetFileMapper extends Mapper<LongWritable, Text, Text, Text>
	{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	    	
	        String line = value.toString();
	        String[] words = line.split(",");      //Splitting record based on ',' for .csv file
	       
	        int wordLength =  words.length;
	        
	        String User_Login = words[wordLength-1];
	        String TextOut = "TWTFL" ;
	        
	        
	        for(int i=0;i< wordLength-1; ++i)
	        {    	
	        	TextOut = TextOut + " " + words[i].trim();	
	        }
	        
	        context.write(new Text(User_Login), new Text(TextOut) );
	    }
	 }
	// End of 	Mapper for Mapping out the key-Value of 'tweets.csv'
	
	
	//---------------------------------------------------------------------------------//	
	
	
	// Mapper for Mapping out the key-Value of 'users.csv'
    
	public static class UsersFileMapper extends Mapper<LongWritable, Text, Text, Text>
	{

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	    {
		    	
		        String line = value.toString();
		        String[] words = line.split(",");      //Splitting record based on ',' for .csv file
		       
		        int wordLength =  words.length;
		        
		        String User_Login = words[0];
		        
		        String TextOut = "USRFL" ;
		       
		        
		        for(int i=1 ; i< wordLength; ++i)
		        {    	
		        	TextOut = TextOut + " " + words[i].trim();	
		        }
		        
		        context.write(new Text(User_Login), new Text(TextOut) );
		        
		        
		        
	    }
	}
	// End of 	Mapper for Mapping out the key-Value of 'users.csv'
			
	//---------------------------------------------------------------------------------//

	//  Beginning The Reducer
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{

	 	public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException 
	    {
			// For each record check the file source; Store User Information and Store tweets info in a string 
			// to be split later using "\t" seperator
			
			String U_login = key.toString();
			String U_Name_And_State="";
			
			
	    	while( values.iterator().hasNext())
	    	{
	    		String Master_Tweets = "";
	    		String Sentence = values.iterator().next().toString();
	    		String[] words = Sentence.split(" ");
	    		
	    		if( words[0]=="TWTFL")
	    		{
	    			String Tweetid_And_TweetMsg = "";
	    			
	    			for(int i = 1; i< words.length; ++i)
	    			{
	    				Tweetid_And_TweetMsg = Tweetid_And_TweetMsg + words.toString();
	    				
	    			}
	    			Master_Tweets = Master_Tweets + Tweetid_And_TweetMsg+"\t";
	    		}
	    		else if (words[0]=="USRFL")
	    		{
	    		  			
	    			for(int i = 1; i< words.length; ++i)
	    			{
	    				U_Name_And_State = U_Name_And_State + words.toString();
	    				
	    			}
	    		}
	    		
	    		// To split the Master tweet file and print output after concatenation with User Info
	    		
	    		String[] tweets = Master_Tweets.toString().split("[ \t]+");
	    		
	    		for(String tweet:tweets)
	    		{
	    			context.write(new Text(U_login), new Text(U_Name_And_State+tweet));
	    		}
	    	}
	    }
	 }
	//	End Of Reducer
	
	
	//----------------------------------------------------------------------------//
	
	
	// The Driver
	
	
	
	 public static void main(String[] args) throws Exception 
	 {
		
		JobConf conf = new JobConf();
		Job job = new Job(conf, "TweetJoin");
		
		job.setJarByClass(Tweet_join.class);
			
	    conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        job.setReducerClass(Reducer.class);
        
       
        MultipleInputs.addInputPath(conf, new Path(args[0]),TextInputFormat.class , TweetFileMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]),TextInputFormat.class , UsersFileMapper.class);
        
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
		  
        job.waitForCompletion(true);
	}
		  
	
	
	
} 
	
