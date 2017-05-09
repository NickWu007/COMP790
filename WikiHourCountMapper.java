// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiHourCountMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(" ");
    if (tokens.length < 3) return;
    String searchTerm = getTerm(tokens[1]);
    int count = Integer.parseInt(tokens[2]);
        
    if (searchTerm != null) context.write(new Text(searchTerm), new IntWritable(count));
  }

  private String getTerm(String fullUrl) {
    if (fullUrl.indexOf(':') >= 0) {
      int start = fullUrl.indexOf(':');
      int end = fullUrl.indexOf('/');
      if (start >= end) return null;
      String projectName = fullUrl.substring(0, start);
      if (projectName.indexOf('%') != -1 || projectName.length() != 2) return null;
      String termName = fullUrl.substring(start, end);
      if (termName.indexOf('%') != -1 || termName.length() != 2) return null;
      return projectName + termName;
    } else {
      if (fullUrl.indexOf('%') == -1 && fullUrl.length() != 2) {
        return fullUrl;
      } else {
        return null;
      }
    }
  }

}

