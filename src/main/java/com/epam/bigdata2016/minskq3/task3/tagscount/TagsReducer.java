package com.epam.bigdata2016.minskq3.task3.tagscount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class TagsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = StreamSupport.stream(values.spliterator(), false)
                .map(IntWritable::get)
                .reduce(0, (res, numb) -> res + numb);
        result.set(sum);
        context.write(key, result);
    }
}
