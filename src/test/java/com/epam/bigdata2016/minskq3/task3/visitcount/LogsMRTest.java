package com.epam.bigdata2016.minskq3.task3.visitcount;


import com.epam.bigdata2016.minskq3.task3.visitcount.model.UserLog;
import eu.bitwalker.useragentutils.Browser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class LogsMRTest {

    String[] input = new String[]{
            "8c66f1538798b7ab57e2da7be11c5696	20130606222224943	Z0KpO7S8PQpNDBa	Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)	192.168.0.*	...	300	250	0	0	100	e1af08818a6cd6bbba118bb54a651961    150	3476	282825712806	0",
            "71e9a371b25950af4e28688743260f3b   20130606001907400   VhkSLxSELTuOkGn Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) 192.168.1.* ... 300 250 0   0   100 e1af08818a6cd6bbba118bb54a651961    150 3476    282825712806    0",
            "71e9a371b25950af4e28688743260f3b   20130606001907400   VhkSLxSELTuOkGn Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) 192.168.1.* ... 300 250 0   0   100 e1af08818a6cd6bbba118bb54a651961    150 3476    282825712806    0"
    };

    String[] ips = new String[]{
            "192.168.0.*",
            "192.168.1.*"
    };


    MapDriver<Object, Text, Text, UserLog> mapDriver;
    ReduceDriver<Text, UserLog, Text, UserLog> reduceDriver;
    MapReduceDriver<Object, Text, Text, UserLog, Text, UserLog> mapReduceDriver;

    @Before
    public void setUp() {
        VisitMapper mapper = new VisitMapper();
        VisitReducer reducer = new VisitReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        Arrays.stream(input).forEach(str -> {
            mapDriver.withInput(new LongWritable(), new Text(str));
        });

        Stream.of(0, 1, 1).forEach(i -> {
            mapDriver.withOutput(new Text(ips[i]), new UserLog(1, 150));
        });

        mapDriver.runTest();

        long ie9 = mapDriver.getCounters().findCounter(Browser.IE9).getValue();
        assertEquals("Counter IE9 should be 3", 3, ie9);
    }

    @Test
    public void testReducer() throws IOException {
        List<UserLog> values1 = new ArrayList<>();
        values1.add(new UserLog(1, 150));
        List<UserLog> values2 = new ArrayList<>();
        values2.add(new UserLog(1, 150));
        values2.add(new UserLog(1, 150));

        reduceDriver.withInput(new Text(ips[0]), values1);
        reduceDriver.withInput(new Text(ips[1]), values2);

        reduceDriver.withOutput(new Text(ips[0]), new UserLog(1, 150));
        reduceDriver.withOutput(new Text(ips[1]), new UserLog(2, 300));

        reduceDriver.runTest();

    }


    @Test
    public void testMapReduce() throws IOException {
        Arrays.stream(input).forEach(str -> {
            mapReduceDriver.withInput(new LongWritable(), new Text(str));
        });

        mapReduceDriver.withOutput(new Text(ips[0]), new UserLog(1, 150));
        mapReduceDriver.withOutput(new Text(ips[1]), new UserLog(2, 300));

        mapReduceDriver.runTest();
    }


}
