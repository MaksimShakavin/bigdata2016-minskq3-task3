package com.epam.bigdata2016.minskq3.task3.visitcount;


import com.epam.bigdata2016.minskq3.task3.visitcount.model.UserLog;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VisitMapper extends Mapper<Object, Text, Text, UserLog> {

    Pattern pIp = Pattern.compile("\\s\\d+\\.\\d+\\.\\d+\\.(\\d+|\\*)\\s");
    private Text ipText = new Text();
    private UserLog vsc = new UserLog();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] params = line.split("\\s+");
        Matcher m = pIp.matcher(line);
        if (m.find()) {
            String ip = m.group().trim();
            ipText.set(ip);
            Integer bp = Integer.parseInt(params[params.length - 4]);

            vsc.setSpendsCount(bp);
            vsc.setVisitsCount(1);
            context.write(ipText, vsc);

            UserAgent ua = new UserAgent(line);
            context.getCounter(ua.getBrowser()).increment(1);
        }
    }
}
