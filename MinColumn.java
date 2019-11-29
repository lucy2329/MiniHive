import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang3.StringUtils;
import java.util.Iterator; 

public class MinColumn {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
      
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
        String row = value.toString();
        String[] rowElems = row.split(",");
        String colString="";
        int counter = 0;
  if (!StringUtils.isNumeric(rowElems[0])) return;
    if (Integer.parseInt(rowElems[0]) > 2) {
          context.write(new Text(""), new IntWritable(Integer.parseInt(rowElems[0])));
            }
      }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int min = Integer.MAX_VALUE;
      Iterator<IntWritable> iterator = values.iterator();
      while (iterator.hasNext()) {

          int value = iterator.next().get();

        if (value < min) { //Finding min value

          min = value;

        }
    } 
    System.out.println("MINIMUM VALUE IS"+min);
    context.write(new Text(key), new IntWritable(min));

    }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(MinColumn.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
