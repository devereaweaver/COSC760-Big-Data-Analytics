import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightsByCarrierReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
   protected void reduce(Text token, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
     
      int sum = 0;
     
      for (IntWritable count : counts) {   // Aggregate the shuffled values
         sum += count.get();
      }
      // Write key (token for carrier) and value as the total number of flights for the particular carrier
      context.write(token, new IntWritable(sum));  // Context object: where output key/value pairs are written.

   }  // end of protected map()
} // end of class