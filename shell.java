import java.io.*;
import java.util.Scanner;

class shell {

    //EXEC LOAD START
    public static void execload(String filename) {
        String cmd = "hadoop fs -put " + filename;
        String s = null;


        try {
            Process q = Runtime.getRuntime().exec(cmd);
            BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(q.getInputStream()));

            BufferedReader stdError1 = new BufferedReader(new InputStreamReader(q.getErrorStream()));


            System.out.println("Here is the standard output of load command:\n");
            while ((s = stdInput1.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of load command (if any):\n");
            while ((s = stdError1.readLine()) != null) {
                System.out.println(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // EXEC LOAD DONE -------------------------------------------------



    // EXEC SELECT , WHERE here 
    public static void execselect(Integer[] columnNumbers, int columnLength, int whereCondition, String whereStr, String checkString) {
        String whereString = "";
        String s = null;
        String cmd = "";

        if (whereCondition == 0) {
            whereString = "1==1";
        } else {
            whereString = whereStr;
        }
        int i = 0;
        String outPutCommand = "";
        while (i <= columnLength) {
            if (i == columnLength)
                outPutCommand = outPutCommand.concat("rowElems[" + columnNumbers[i] + "]");
            else
                outPutCommand = outPutCommand.concat("rowElems[" + columnNumbers[i] + "]+" + "\"|\"+");
            i += 1;
        }
        System.out.println(outPutCommand);
        String soloselect_code = "import java.io.IOException;import java.util.StringTokenizer;import org.apache.hadoop.conf.Configuration;import org.apache.commons.lang3.StringUtils;import org.apache.hadoop.fs.Path;import org.apache.hadoop.io.IntWritable;import org.apache.hadoop.io.Text;import org.apache.hadoop.mapreduce.Job;import org.apache.hadoop.mapreduce.Mapper;import org.apache.hadoop.mapreduce.Reducer;import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;public class SelectColumn {  public static class TokenizerMapper       extends Mapper<Object, Text, Text, IntWritable>{    private final static IntWritable one = new IntWritable(1);    private Text word = new Text();    public void map(Object key, Text value, Context context                        ) throws IOException, InterruptedException {        String row = value.toString();        String[] rowElems = row.split(\",\");" + checkString + "        if(" + whereString + ")        {             context.write(new Text(" + outPutCommand + "),one);        }         }  }  public static class IntSumReducer       extends Reducer<Text,IntWritable,Text,IntWritable> {    private final static IntWritable one = new IntWritable(1);    public void reduce(Text key, Iterable<IntWritable> values,                       Context context                       ) throws IOException, InterruptedException {      for (IntWritable val : values) {        context.write(key, one);      }    }  }  public static void main(String[] args) throws Exception {    Configuration conf = new Configuration();    Job job = Job.getInstance(conf, \"select column\");    job.setJarByClass(SelectColumn.class);    job.setMapperClass(TokenizerMapper.class);    job.setCombinerClass(IntSumReducer.class);    job.setReducerClass(IntSumReducer.class);    job.setOutputKeyClass(Text.class);    job.setOutputValueClass(IntWritable.class);    FileInputFormat.addInputPath(job, new Path(args[0]));    FileOutputFormat.setOutputPath(job, new Path(args[1]));    System.exit(job.waitForCompletion(true) ? 0 : 1);  }}";

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("SelectColumn.java"));
            writer.write(soloselect_code);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            cmd = "hadoop com.sun.tools.javac.Main SelectColumn.java";
            Process q = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(q.getInputStream()));

            BufferedReader stdError1 = new BufferedReader(new InputStreamReader(q.getErrorStream()));


            // read the output from the command
            System.out.println("Here is the standard output of the java select compile command:\n");
            while ((s = stdInput1.readLine()) != null) {
                System.out.println(s);
            }

            // read any errors from the attempted command
            System.out.println("Here is the standard error of the java select compile command (if any):\n");
            while ((s = stdError1.readLine()) != null) {
                System.out.println(s);
            }


            long sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }
            //-------------------------------JAR COMMAND -------------------------------------------------------------------------------------------------------------------

            cmd = "jar cf SelectCol.jar SelectColumn.class SelectColumn$IntSumReducer.class SelectColumn$TokenizerMapper.class";
            Process r = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput2 = new BufferedReader(new InputStreamReader(r.getInputStream()));

            BufferedReader stdError2 = new BufferedReader(new InputStreamReader(r.getErrorStream()));
            // read the output from the command
            System.out.println("Here is the standard output of the select jar command:\n");
            while ((s = stdInput2.readLine()) != null) {
                System.out.println(s);
            }

            // read any errors from the attempted command
            System.out.println("Here is the standard error of the select jar command (if any):\n");
            while ((s = stdError2.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop jar SelectCol.jar SelectColumn /minihive /output";
            Process z = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput3 = new BufferedReader(new InputStreamReader(z.getInputStream()));
            BufferedReader stdError3 = new BufferedReader(new InputStreamReader(z.getErrorStream()));
            // read the output from the command
            System.out.println("Here is the standard output of the select run on hadoop:\n");
            while ((s = stdInput3.readLine()) != null) {
                System.out.println(s);
            }

            // read any errors from the attempted command
            System.out.println("Here is the standard error of the select run on hadoop (if any):\n");
            while ((s = stdError3.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop fs -cat /output/part-r-00000";
            System.out.println("COMMAND" + cmd);
            Process b = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput4 = new BufferedReader(new InputStreamReader(b.getInputStream()));
            BufferedReader stdError4 = new BufferedReader(new InputStreamReader(b.getErrorStream()));
            // read the output from the command
            System.out.println("Here is the standard output of the select cat command:\n");
            while ((s = stdInput4.readLine()) != null) {
                System.out.println(s);
            }

            // read any errors from the attempted command
            System.out.println("Here is the standard error of the select cat command(if any):\n");
            while ((s = stdError4.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop fs -rm -r /output";
            Process c = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput5 = new BufferedReader(new InputStreamReader(c.getInputStream()));
            BufferedReader stdError5 = new BufferedReader(new InputStreamReader(c.getErrorStream()));

            System.out.println("Here is the standard output of the select rm command:\n");
            while ((s = stdInput5.readLine()) != null) {
                System.out.println(s);
            }


            System.out.println("Here is the standard error of the select rm command (if any):\n");
            while ((s = stdError5.readLine()) != null) {
                System.out.println(s);
            }
        } //end of try
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    // END OF EXEC SELECT WITH WHERE 

    // START OF SELECT COUNT
    public static void execcount(int colnumber,  int whereCondition, String whereStr, String checkString) {
        String whereString = "";
        String s = null;
        String cmd = "";

        if (whereCondition == 0) {
            whereString = "1==1";
        } else {
            whereString = whereStr;
        }

        String solocount_code = "import java.io.IOException;import java.util.StringTokenizer;import org.apache.hadoop.conf.Configuration;import org.apache.commons.lang3.StringUtils;import org.apache.hadoop.fs.Path;import org.apache.hadoop.io.IntWritable;import org.apache.hadoop.io.Text;import org.apache.hadoop.mapreduce.Job;import org.apache.hadoop.mapreduce.Mapper;import org.apache.hadoop.mapreduce.Reducer;import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;public class CountColumn {  public static class TokenizerMapper       extends Mapper<Object, Text, Text, IntWritable>{    private final static IntWritable one = new IntWritable(1);    private Text word = new Text();    public void map(Object key, Text value, Context context                        ) throws IOException, InterruptedException {        String row = value.toString();        String[] rowElems = row.split(\",\");        " + checkString + "if(" + whereString + ")        {             context.write(new Text(rowElems[" + colnumber + "]),one);        }         }  }  public static class IntSumReducer       extends Reducer<Text,IntWritable,Text,IntWritable> {    private IntWritable result = new IntWritable();    public void reduce(Text key, Iterable<IntWritable> values,                       Context context                       ) throws IOException, InterruptedException {      int sum = 0;      for (IntWritable val : values) {        sum += val.get();      }      result.set(sum);      context.write(key, result);    }  }  public static void main(String[] args) throws Exception {    Configuration conf = new Configuration();    Job job = Job.getInstance(conf, \"word count\");    job.setJarByClass(CountColumn.class);    job.setMapperClass(TokenizerMapper.class);    job.setCombinerClass(IntSumReducer.class);    job.setReducerClass(IntSumReducer.class);    job.setOutputKeyClass(Text.class);    job.setOutputValueClass(IntWritable.class);    FileInputFormat.addInputPath(job, new Path(args[0]));    FileOutputFormat.setOutputPath(job, new Path(args[1]));    System.exit(job.waitForCompletion(true) ? 0 : 1);  }}";

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("CountColumn.java"));
            writer.write(solocount_code);
            writer.close();

            cmd = "hadoop com.sun.tools.javac.Main CountColumn.java";
            Process q = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(q.getInputStream()));

            BufferedReader stdError1 = new BufferedReader(new InputStreamReader(q.getErrorStream()));

            System.out.println("Here is the standard output of the java select compile command:\n");
            while ((s = stdInput1.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of the java select compile command (if any):\n");
            while ((s = stdError1.readLine()) != null) {
                System.out.println(s);
            }

            long sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }
            //-------------------------------JAR COMMAND -------------------------------------------------------------------------------------------------------------------

            cmd = "jar cf CountCol.jar CountColumn.class CountColumn$IntSumReducer.class CountColumn$TokenizerMapper.class";
            Process r = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput2 = new BufferedReader(new InputStreamReader(r.getInputStream()));

            BufferedReader stdError2 = new BufferedReader(new InputStreamReader(r.getErrorStream()));

            System.out.println("Here is the standard output of the select jar command:\n");
            while ((s = stdInput2.readLine()) != null) {
                System.out.println(s);
            }


            System.out.println("Here is the standard error of the select jar command (if any):\n");
            while ((s = stdError2.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop jar CountCol.jar CountColumn /minihive /output";
            Process z = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput3 = new BufferedReader(new InputStreamReader(z.getInputStream()));
            BufferedReader stdError3 = new BufferedReader(new InputStreamReader(z.getErrorStream()));

            System.out.println("Here is the standard output of the select run on hadoop:\n");
            while ((s = stdInput3.readLine()) != null) {
                System.out.println(s);
            }


            System.out.println("Here is the standard error of the select run on hadoop (if any):\n");
            while ((s = stdError3.readLine()) != null) {
                System.out.println(s);
            }


            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop fs -cat /output/part-r-00000";
            Process b = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput4 = new BufferedReader(new InputStreamReader(b.getInputStream()));
            BufferedReader stdError4 = new BufferedReader(new InputStreamReader(b.getErrorStream()));

            System.out.println("Here is the standard output of the select cat command:\n");
            while ((s = stdInput4.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of the select cat command(if any):\n");
            while ((s = stdError4.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop fs -rm -r /output";
            Process c = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput5 = new BufferedReader(new InputStreamReader(c.getInputStream()));
            BufferedReader stdError5 = new BufferedReader(new InputStreamReader(c.getErrorStream()));

            System.out.println("Here is the standard output of the select count rm command:\n");
            while ((s = stdInput5.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of the select count rm command (if any):\n");
            while ((s = stdError5.readLine()) != null) {
                System.out.println(s);
            }
        } // END OF TRY
        catch (IOException e) {
            e.printStackTrace();
        }

    }
    // END OF SELECT COUNT
    // START OF SELECT SUM
    public static void execsum(int colnumber, int whereCondition, String whereStr, String checkString) {
        String whereString = "";
        String s = null;
        String cmd = "";

        if (whereCondition == 0) {
            whereString = "1==1";
        } else {
            whereString = whereStr;
        }
        String code = "import java.io.IOException;import java.util.StringTokenizer;import org.apache.hadoop.conf.Configuration;import org.apache.hadoop.fs.Path;import org.apache.hadoop.io.IntWritable;import org.apache.hadoop.io.Text;import org.apache.hadoop.mapreduce.Job;import org.apache.hadoop.mapreduce.Mapper;import org.apache.hadoop.mapreduce.Reducer;import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;import org.apache.commons.lang3.StringUtils;public class SumColumn {  public static class TokenizerMapper       extends Mapper<Object, Text, IntWritable, IntWritable>{    private final static IntWritable one = new IntWritable(1);    private Text word = new Text();    public void map(Object key, Text value, Context context                        ) throws IOException, InterruptedException {                String row = value.toString();        String[] rowElems = row.split(\",\");        String colString=\"\";        int counter = 0;     "+checkString + "        if("+whereString+")        {                           context.write(one,new IntWritable(Integer.parseInt(rowElems["+colnumber+"])));        }         }  }  public static class IntSumReducer       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {    private IntWritable result = new IntWritable();    public void reduce(IntWritable key, Iterable<IntWritable> values,                       Context context                       ) throws IOException, InterruptedException {     int sum = 0;      IntWritable one = new IntWritable(1);      for (IntWritable val : values)        {                    sum += val.get();        }      result.set(sum);      context.write(one, result);    }}  public static void main(String[] args) throws Exception {    Configuration conf = new Configuration();    Job job = Job.getInstance(conf, \"word count\");    job.setJarByClass(SumColumn.class);    job.setMapperClass(TokenizerMapper.class);    job.setCombinerClass(IntSumReducer.class);    job.setReducerClass(IntSumReducer.class);    job.setOutputKeyClass(IntWritable.class);    job.setOutputValueClass(IntWritable.class);    FileInputFormat.addInputPath(job, new Path(args[0]));    FileOutputFormat.setOutputPath(job, new Path(args[1]));    System.exit(job.waitForCompletion(true) ? 0 : 1);  }}";
                
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("SumColumn.java"));
            writer.write(code);
            writer.close();

            cmd = "hadoop com.sun.tools.javac.Main SumColumn.java";
            Process q = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput1 = new BufferedReader(new InputStreamReader(q.getInputStream()));

            BufferedReader stdError1 = new BufferedReader(new InputStreamReader(q.getErrorStream()));

            System.out.println("Here is the standard output of the java select sum compile command:\n");
            while ((s = stdInput1.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of the java select sum compile command (if any):\n");
            while ((s = stdError1.readLine()) != null) {
                System.out.println(s);
            }

            long sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }
            //-------------------------------JAR COMMAND -------------------------------------------------------------------------------------------------------------------

            cmd = "jar cf SumCol.jar SumColumn.class SumColumn$IntSumReducer.class SumColumn$TokenizerMapper.class";
            Process r = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput2 = new BufferedReader(new InputStreamReader(r.getInputStream()));

            BufferedReader stdError2 = new BufferedReader(new InputStreamReader(r.getErrorStream()));

            System.out.println("Here is the standard output of the select sum jar command:\n");
            while ((s = stdInput2.readLine()) != null) {
                System.out.println(s);
            }


            System.out.println("Here is the standard error of the select sum jar command (if any):\n");
            while ((s = stdError2.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop jar SumCol.jar SumColumn /minihive /output";
            Process z = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput3 = new BufferedReader(new InputStreamReader(z.getInputStream()));
            BufferedReader stdError3 = new BufferedReader(new InputStreamReader(z.getErrorStream()));

            System.out.println("Here is the standard output of the select sum run on hadoop:\n");
            while ((s = stdInput3.readLine()) != null) {
                System.out.println(s);
            }


            System.out.println("Here is the standard error of the select sum run on hadoop (if any):\n");
            while ((s = stdError3.readLine()) != null) {
                System.out.println(s);
            }


            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop fs -cat /output/part-r-00000";
            Process b = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput4 = new BufferedReader(new InputStreamReader(b.getInputStream()));
            BufferedReader stdError4 = new BufferedReader(new InputStreamReader(b.getErrorStream()));

            System.out.println("Here is the standard output of the select sum cat command:\n");
            while ((s = stdInput4.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of the select sum cat command(if any):\n");
            while ((s = stdError4.readLine()) != null) {
                System.out.println(s);
            }

            sleeper = 10000000L;
            while (sleeper > 0) {
                sleeper--; //delay
            }

            cmd = "hadoop fs -rm -r /output";
            Process c = Runtime.getRuntime().exec(cmd);

            BufferedReader stdInput5 = new BufferedReader(new InputStreamReader(c.getInputStream()));
            BufferedReader stdError5 = new BufferedReader(new InputStreamReader(c.getErrorStream()));

            System.out.println("Here is the standard output of the select sum rm command:\n");
            while ((s = stdInput5.readLine()) != null) {
                System.out.println(s);
            }

            System.out.println("Here is the standard error of the select sum rm command (if any):\n");
            while ((s = stdError5.readLine()) != null) {
                System.out.println(s);
            }
        } // END OF TRY
        catch (IOException e) {
            e.printStackTrace();
        }

    }
    // END OF SELECT SUM

    public static void main(String[] args) {

        while (true) {
            String s = null;
            Integer load = new Integer(0);
            Integer wordcount = new Integer(0);
            Integer soloselect = new Integer(0);
            Integer solocount = new Integer(0);

            System.out.print("minihive$>");

            Scanner scanner = new Scanner(System.in);
            String cmd = scanner.nextLine();
            String folder_name = null;

            if (cmd.equals("quit")) {
                System.exit(0);
            }

            String[] parsed = cmd.split(" ");
            System.out.println(parsed[0]);

            if (parsed[0].equals("LOAD")) {
                cmd = "hadoop fs -put " + parsed[1] + " /minihive";
                load = 1;
                System.out.println(parsed[1]);
                String fileContent = "";

                int i = 3;
                while (i < parsed.length) {
                    parsed[i] = parsed[i].replace("(", "");
                    parsed[i] = parsed[i].replace(";", "");
                    parsed[i] = parsed[i].replace(")", "");
                    parsed[i] = parsed[i].replace(",", "");
                    //System.out.println(parsed[i]);
                    fileContent = fileContent + parsed[i] + ",";
                    i++;
                }

                try {
                    String filename = parsed[1].replace(".csv", "");
                    BufferedWriter writer = new BufferedWriter(new FileWriter(filename + "_schema.txt"));
                    writer.write(fileContent);
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }


                System.out.println("inside parsed :" + parsed[1]);
                String filename = parsed[1].replace(".csv", "");


                execload(parsed[1] + " /minihive");
                execload(filename + "_schema.txt /minihive");

            }
            // LOAD DONE

            String colname = parsed[1];
            String tablename = parsed[3];


            //START OF SELECT without COUNT
            if (parsed[0].equals("SELECT") && parsed[1].indexOf("COUNT") != 0 && parsed[1].indexOf("SUM") != 0) {

                try {

                    String schema = "";
                    BufferedReader br = new BufferedReader(new FileReader(tablename + "_schema.txt"));

                    int whereCondition = 0;
                    schema = br.readLine();
                    String[] cols;
                    int maxCols = 0;
                    String[] colNumList = colname.split(",");
                    Integer[] columnNumbers = new Integer[100];
                    int counterList = 0;
                    String[] parsedQuery = schema.split(",");
                    soloselect = 1;
                    Integer columnLength = 0;
                    Integer colnumber = new Integer(-1);
                    String checkString = "";
                    String whereString = "";
                    int isString=0;
                    //select a from tbl where b < 5
                    if (parsed.length > 4) {
                        checkString = "if(!StringUtils.isNumeric(rowElems[2])) return;";
                        whereCondition = 1;
                        int counter = 0;
                        while (counter < parsedQuery.length) {

                            cols = parsedQuery[counter].split("=");
                            if (cols[0].equals(parsed[5])) {
                                colnumber = counter;
                                if(cols[1].equals("str"))isString = 1;
                                break;
                            }
                            counter++;
                        }
                        if(isString ==1){
                         	checkString = "";
                         	String[] quotes = cmd.split("\"");
 	                        whereString = "rowElems[" + colnumber + "].equals(\"" + quotes[1]+"\")";
                         }
                         else{
                        whereString = "Integer.parseInt(rowElems[" + colnumber + "])" + parsed[6] + parsed[7];
                    	}
            	}

                    colnumber = -1;
                    System.out.println(colNumList[0]);
                    while (counterList < colNumList.length) {
                        int counter = 0;
                        while (counter < parsedQuery.length) {

                            cols = parsedQuery[counter].split("=");
                            System.out.println(cols[0] + "$" + colNumList[counterList]);
                            if (cols[0].equals(colNumList[counterList])) {
                                columnNumbers[counterList] = counter;
                                colnumber = counter;
                                break;
                            }
                            counter++;
                        }
                        counterList += 1;
                    }
                    columnLength = counterList - 1;
                    execselect(columnNumbers, columnLength, whereCondition, whereString, checkString);
                    //^ Executing select
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } // END OF IF SELECT without count
            // start of select with count
            else if (parsed[0].equals("SELECT") && parsed[1].indexOf("COUNT") == 0) {
                solocount = 1;
                colname = parsed[1].split("\\(")[1].replace(")", "");
                System.out.println(colname);
                Integer colnumber = new Integer(-1);
		          String checkString = "";
            String whereString = "";;
		Integer whereCondition = 0;
int isString=0;
                try {
                    BufferedReader br = new BufferedReader(new FileReader(tablename + "_schema.txt"));

                    String schema = "";
                    schema = br.readLine();
                    String[] cols;
                    //System.out.println(schema);

                    String[] parsedQuery = schema.split(",");
                    int counter = 0;

                    while (counter < parsedQuery.length) {
                        cols = parsedQuery[counter].split("=");
                        //System.out.println(cols[0]+"$"+cols[1]);
                        if (cols[0].equals(colname)) {
                            colnumber = counter;
                            break;
                        }
                        counter++;
                    }

		    if (parsed.length > 4) {
                        checkString = "if(!StringUtils.isNumeric(rowElems[2])) return;";
                        whereCondition = 1;
                        counter = 0;
                        while (counter < parsedQuery.length) {

                            cols = parsedQuery[counter].split("=");
                            if (cols[0].equals(parsed[5])) {
                                colnumber = counter;
                                if(cols[1].equals("str"))isString = 1;
                                break;
                            }
                            counter++;
                        }
                        if(isString ==1){
                         	checkString = "";
                         	String[] quotes = cmd.split("\"");
 	                        whereString = "rowElems[" + colnumber + "].equals(\"" + quotes[1]+"\")";
                         }
                         else{
                        whereString = "Integer.parseInt(rowElems[" + colnumber + "])" + parsed[6] + parsed[7];
                    	}
            	}

                    execcount(colnumber, whereCondition, whereString, checkString);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //end of select with count
            //start of select with sum
            else if (parsed[0].equals("SELECT") && parsed[1].indexOf("SUM") == 0){
                System.out.println("HERE");
                colname = parsed[1].split("\\(")[1].replace(")", "");
                System.out.println(colname);
                Integer colnumber = new Integer(-1);
                try {
                    BufferedReader br = new BufferedReader(new FileReader(tablename + "_schema.txt"));

                    String schema = "";
                    schema = br.readLine();
                    String[] cols;
                    //System.out.println(schema);

                    String[] parsedQuery = schema.split(",");
                    int counter = 0;

                   

                    String whereString = "";;
                    Integer whereCondition = 0;
                    int isString=0;
                    String checkString = "";
                    if (parsed.length > 4) {  

                        checkString = "if(!StringUtils.isNumeric(rowElems[2])) return;";                      
                        whereCondition = 1;
                        counter = 0;
                        while (counter < parsedQuery.length) {

                            cols = parsedQuery[counter].split("=");
                            if (cols[0].equals(parsed[5])) {
                                colnumber = counter;
                                if(cols[1].equals("str"))
                                    isString = 1;
                                break;
                            }
                            counter++;
                        }
                        if(isString ==1){
                            checkString = "";
                            String[] quotes = cmd.split("\"");
                            whereString = "rowElems[" + colnumber + "].equals(\"" + quotes[1]+"\")";
                         }
                        else{
                        whereString = "Integer.parseInt(rowElems[" + colnumber + "])" + parsed[6] + parsed[7];
                        }
                }

                 while (counter < parsedQuery.length) {
                        cols = parsedQuery[counter].split("=");
                        //System.out.println(cols[0]+"$"+cols[1]);
                        if (cols[0].equals(colname)) {
                            colnumber = counter;
                            break;
                        }
                        counter++;
                    }

                    execsum(colnumber, whereCondition, whereString,  checkString);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //end of select with sum
            else {
                try {
                    Process p = Runtime.getRuntime().exec(cmd);

                    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));

                    BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));


                    System.out.println("Here is the standard output of the command:\n");
                    while ((s = stdInput.readLine()) != null) {
                        System.out.println(s);
                    }


                    System.out.println("Here is the standard error of the command (if any):\n");
                    while ((s = stdError.readLine()) != null) {
                        System.out.println(s);
                    }


                } // END OF TRY
                catch (IOException e) {
                    System.out.println("exception happened - here's what I know: ");
                    e.printStackTrace();
                    System.exit(-1);
                }

            } // end of else

        }
    }

}
