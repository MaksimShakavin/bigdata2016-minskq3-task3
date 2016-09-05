package com.epam.bigdata2016.minskq3.task3.visitcount;

import com.epam.bigdata2016.minskq3.task3.visitcount.model.UserLog;
import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: VisitsSpendsCount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Visits Spends count");
        job.setJarByClass(Main.class);
        job.setMapperClass(VisitMapper.class);

        // for current task Combiner is the same like Reducer
        // take a look at http://www.tutorialspoint.com/map_reduce/map_reduce_combiners.htm
        job.setCombinerClass(VisitReducer.class);
        job.setReducerClass(VisitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserLog.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean result = job.waitForCompletion(true);

        System.out.println("Browsers Counter :");
        for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
            System.out.println(" - " + counter.getDisplayName() + ": " + counter.getValue());
        }

        System.exit(result ? 0 : 1);
    }
}
