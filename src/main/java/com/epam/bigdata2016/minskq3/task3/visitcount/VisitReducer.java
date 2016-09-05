package com.epam.bigdata2016.minskq3.task3.visitcount;

import com.epam.bigdata2016.minskq3.task3.visitcount.model.UserLog;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.stream.StreamSupport;


public class VisitReducer extends Reducer<Text, UserLog, Text, UserLog> {
    private UserLog result = new UserLog();

    public void reduce(Text key, Iterable<UserLog> values, Context context) {
        try {
            result.setVisitsCount(0L);
            result.setSpendsCount(0L);
            StreamSupport.stream(values.spliterator(), false)
                    .forEach(log -> {
                        result.addSpends(log.getSpendsCount());
                        result.addVisits(log.getVisitsCount());
                    });
            context.write(key, result);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

}
