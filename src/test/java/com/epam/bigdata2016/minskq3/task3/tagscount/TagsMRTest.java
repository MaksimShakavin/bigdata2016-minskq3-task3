package com.epam.bigdata2016.minskq3.task3.tagscount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TagsMRTest {

    private final String[] input = new String[]{
            "header",
            "282163091263 audi,bmw,mercedes,mazda ON CPC BROAD http://dummy.com",
            "282163091263 mercedes,mazda ON CPC BROAD http://dummy.com",
            "282163091263 mazda ON CPC BROAD http://dummy.com"
    };
    private final String[] values = new String[]{
            "AUDI", "BMW", "MERCEDES", "MAZDA"
    };
    private final String header = "header";
    private final String input1 = "282163091263 audi,bmw,mercedes,mazda ON CPC BROAD http://dummy.com";
    private final String input2 = "282163091263 mercedes,mazda ON CPC BROAD http://dummy.com";
    private final String input3 = "282163091263 mazda ON CPC BROAD http://dummy.com";
    private final String s1 = "AUDI";
    private final String s2 = "BMW";
    private final String s3 = "MERCEDES";
    private final String s4 = "MAZDA";
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        TagsMapper mapper = new TagsMapper();
        TagsReducer reducer = new TagsReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        IntStream.range(0, input.length).forEach(i ->
                mapDriver.withInput(new LongWritable(i), new Text(input[i]))
        );

        Stream.of(0, 1, 2, 3, 2, 3, 3).forEach(i ->
                mapDriver.withOutput(new Text(values[i]), new IntWritable(1))
        );

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s1), values1);

        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s2), values2);

        List<IntWritable> values3 = new ArrayList<IntWritable>();
        values3.add(new IntWritable(1));
        values3.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s3), values3);

        List<IntWritable> values4 = new ArrayList<IntWritable>();
        values4.add(new IntWritable(1));
        values4.add(new IntWritable(1));
        values4.add(new IntWritable(1));
        reduceDriver.withInput(new Text(s4), values4);

        reduceDriver.withOutput(new Text(s1), new IntWritable(1));
        reduceDriver.withOutput(new Text(s2), new IntWritable(1));
        reduceDriver.withOutput(new Text(s3), new IntWritable(2));
        reduceDriver.withOutput(new Text(s4), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        IntStream.range(0, input.length).forEach(i -> {
            mapDriver.withInput(new LongWritable(i), new Text(input[i]));
        });

        mapReduceDriver.withOutput(new Text(s1), new IntWritable(1));
        mapReduceDriver.withOutput(new Text(s2), new IntWritable(1));
        mapReduceDriver.withOutput(new Text(s4), new IntWritable(3));
        mapReduceDriver.withOutput(new Text(s3), new IntWritable(2));

        mapReduceDriver.runTest();
    }
}