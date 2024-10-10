import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlightsByCarrier {
   public static void main(String[] args) throws Exception {
      Job job = new Job();
      job.setJarByClass(FlightsByCarrier.class);
      job.setJobName("FlightsByCarrier");
      
      // Identifying HDFS pathe for the input data
      TextInputFormat.addInputPath(job, new Path(args[0]));
      job.setInputFormatClass(TextInputFormat.class);  //the expected format of the data, by default, input format is TextInputFormat
      
      //Define the overall structure of the MapReduce application
      job.setMapperClass(FlightsByCarrierMapper.class); // Specify both the Mapper and Reducer classes
      job.setReducerClass(FlightsByCarrierReducer.class);
      
      // Indicate the HDFS path for the application's output as well as the format of the data
      TextOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
    
      // Run the job and wait
      job.waitForCompletion(true);  // The driver waits at this point unitl the waitForCompletion function returns
      
   } // end of main
  
} // end of class FlightsByCarrier