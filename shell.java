import java.io.*;
import java.util.Scanner;

class shell {
	public static void main(String[] args)
	{
		
		while(true)
		{
			String s = null;
			Integer load = new Integer(0);
			Integer wordcount = new Integer(0);
			
			
			System.out.print("hive$>");			
			
			Scanner scanner = new Scanner(System.in);			
	                String cmd = scanner.nextLine();
	                String folder_name = null;
	                
	                if(cmd.equals("quit"))
	                {
	                	System.exit(0);
	                }
	                
	                String[] parsed = cmd.split(" ");
	                
	                if(parsed[0].equals("LOAD"))
	                {
	                	cmd = "hadoop fs -put "+parsed[1]+" /minihive";
	                	load = 1;
	                	System.out.println(parsed[1]);
	                	String fileContent="";
	                	
	                	int i=3;
	                	while(i < parsed.length)
	                	{
	                		parsed[i] = parsed[i].replace("(", "");
            			        parsed[i] = parsed[i].replace(";", "");
            			        parsed[i] = parsed[i].replace(")", "");
            			        parsed[i] = parsed[i].replace(",", "");
                			//System.out.println(parsed[i]);
            				fileContent = fileContent + parsed[i] + ",";
                			i++;
	                	}	                	

     				try
     				{
     					String filename = parsed[1].replace(".csv", "");
     					BufferedWriter writer = new BufferedWriter(new FileWriter(filename+"_schema.txt"));
					writer.write(fileContent);
					writer.close();
     				}
    				catch (IOException e)
    				{
    					e.printStackTrace();
    				}
    				
	                	
	                }
	                
	                if(parsed[0].equals("wordcount"))
	                {
	                	String filename = parsed[1];
	                	wordcount=1;
	                	cmd = "hadoop fs -put "+parsed[1]+" /input";              	
	                	               	
	                }

			try {
			    
			    // run the Unix "ps -ef" command
			    // using the Runtime exec method:
			    Process p = Runtime.getRuntime().exec(cmd);
			    
			    BufferedReader stdInput = new BufferedReader(new 
				 InputStreamReader(p.getInputStream()));

			    BufferedReader stdError = new BufferedReader(new 
				 InputStreamReader(p.getErrorStream()));

			    // read the output from the command
			    System.out.println("Here is the standard output of the command:\n");
			    while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
			    }
			    
			    // read any errors from the attempted command
			    System.out.println("Here is the standard error of the command (if any):\n");
			    while ((s = stdError.readLine()) != null) {
				System.out.println(s);
			    }
			    
			    if(load==1)
			    {
			    	System.out.println("inside parsed :"+parsed[1]);
			    	String filename = parsed[1].replace(".csv", "");
			    	cmd = "hadoop fs -put "+filename+"_schema.txt /minihive";
		    	    	Process q = Runtime.getRuntime().exec(cmd);
		    	    
	   	    	    	BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(q.getInputStream()));

		    		BufferedReader stdError1 = new BufferedReader(new InputStreamReader(q.getErrorStream()));
		    
			    
			    // read the output from the command
				System.out.println("Here is the standard output of the second command:\n");
				while ((s = stdInput1.readLine()) != null) {
					System.out.println(s);
				}
			    
			    // read any errors from the attempted command
			    System.out.println("Here is the standard error of the second command (if any):\n");
				while ((s = stdError1.readLine()) != null) {
					System.out.println(s);
				}
			    }
			    else if(wordcount == 1)
			    {
			    	System.out.println("processing word count: "+parsed[1]);
			    	
			    	String javacode = "import java.io.IOException; import java.util.StringTokenizer;import org.apache.hadoop.conf.Configuration;import org.apache.hadoop.fs.Path;import org.apache.hadoop.io.IntWritable;import org.apache.hadoop.io.Text;import org.apache.hadoop.mapreduce.Job;import org.apache.hadoop.mapreduce.Mapper;import org.apache.hadoop.mapreduce.Reducer;import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; public class WordCount {  public static class TokenizerMapper       extends Mapper<Object, Text, Text, IntWritable>{    private final static IntWritable one = new IntWritable(1);    private Text word = new Text();    public void map(Object key, Text value, Context context                    ) throws IOException, InterruptedException {      StringTokenizer itr = new StringTokenizer(value.toString());      while (itr.hasMoreTokens()) {        word.set(itr.nextToken());        context.write(word, one);      }    }  }  public static class IntSumReducer       extends Reducer<Text,IntWritable,Text,IntWritable> {    private IntWritable result = new IntWritable();    public void reduce(Text key, Iterable<IntWritable> values,                       Context context                       ) throws IOException, InterruptedException {      int sum = 0;      for (IntWritable val : values) {        sum += val.get();      }      result.set(sum);      context.write(key, result);    }  }  public static void main(String[] args) throws Exception {    Configuration conf = new Configuration();    Job job = Job.getInstance(conf, \"word count\");    job.setJarByClass(WordCount.class);    job.setMapperClass(TokenizerMapper.class);    job.setCombinerClass(IntSumReducer.class);    job.setReducerClass(IntSumReducer.class);    job.setOutputKeyClass(Text.class);    job.setOutputValueClass(IntWritable.class);    FileInputFormat.addInputPath(job, new Path(args[0]));    FileOutputFormat.setOutputPath(job, new Path(args[1]));    System.exit(job.waitForCompletion(true) ? 0 : 1);  }}";       			
			    	
			    	try
     				{
     					
     					BufferedWriter writer = new BufferedWriter(new FileWriter("WordCount.java"));
					writer.write(javacode);
					writer.close();
     				}
    				catch (IOException e)
    				{
    					e.printStackTrace();
    				}
			    	
			    	long sleeper=1000000L;
			    	while(sleeper>0){
			    		sleeper--; //delay
			    	}
			    	
			    	
			    	
			    	cmd = "hadoop com.sun.tools.javac.Main WordCount.java";
		    	    	Process q = Runtime.getRuntime().exec(cmd);
		    	    
	   	    	    	BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(q.getInputStream()));

		    		BufferedReader stdError1 = new BufferedReader(new InputStreamReader(q.getErrorStream()));
		    
			    
			    // read the output from the command
				System.out.println("Here is the standard output of the java compile command:\n");
				while ((s = stdInput1.readLine()) != null) {
					System.out.println(s);
				}
			    
			    // read any errors from the attempted command
			    System.out.println("Here is the standard error of the java compile command (if any):\n");
				while ((s = stdError1.readLine()) != null) {
					System.out.println(s);
				}
				
				
				sleeper = 10000000L;
				while(sleeper>0){
			    		sleeper--; //delay
			    	}
				//-------------------------------JAR COMMAND -------------------------------------------------------------------------------------------------------------------
				
				cmd = "jar cf wc.jar WordCount.class WordCount$IntSumReducer.class WordCount$TokenizerMapper.class";
		    	    	Process r = Runtime.getRuntime().exec(cmd);
		    	    
	   	    	    	BufferedReader stdInput2 = new BufferedReader(new InputStreamReader(r.getInputStream()));

		    		BufferedReader stdError2 = new BufferedReader(new InputStreamReader(r.getErrorStream()));
		    	    // read the output from the command
				System.out.println("Here is the standard output of the jar command:\n");
				while ((s = stdInput2.readLine()) != null) {
					System.out.println(s);
				}
			    
			    // read any errors from the attempted command
			    System.out.println("Here is the standard error of the jar command (if any):\n");
				while ((s = stdError2.readLine()) != null) {
					System.out.println(s);
				}
				
				sleeper = 1000000L;
				while(sleeper>0){
			    		sleeper--; //delay
			    	}
				
				cmd = "hadoop jar wc.jar WordCount /input /output";
		    	    	Process z = Runtime.getRuntime().exec(cmd);
		    	    
	   	    	    	BufferedReader stdInput3 = new BufferedReader(new InputStreamReader(z.getInputStream()));
		    		BufferedReader stdError3 = new BufferedReader(new InputStreamReader(z.getErrorStream()));
		    	    // read the output from the command
				System.out.println("Here is the standard output of the run on hadoop:\n");
				while ((s = stdInput3.readLine()) != null) {
					System.out.println(s);
				}
			    
			    // read any errors from the attempted command
			    System.out.println("Here is the standard error of the run on hadoop (if any):\n");
				while ((s = stdError3.readLine()) != null) {
					System.out.println(s);
				}
				
			    }
			    
			    //System.exit(0);
			}
			catch (IOException e) {
			    System.out.println("exception happened - here's what I know: ");
			    e.printStackTrace();
			    System.exit(-1);
			}
			
		}
	}
	
}
