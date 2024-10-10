import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

//import au.com.bytecode.opencsv.CSVParser;

public class FlightsByCarrierMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
   protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     
     // if: skip the first line with the description of each column
     if (key.get() > 0) {
        String[] lines = new CSVParser().parseLine(value.toString());
        // key from the ninth value, representing the carrier
        // value: 1
        context.write(new Text(lines[8]), new IntWritable(1));  // Context object: where output key/value pairs are written.
     }
   }  // end of protected map()
} // end of class