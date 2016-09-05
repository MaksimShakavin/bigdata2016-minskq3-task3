package com.epam.bigdata2016.minskq3.task3.tagscount;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TagsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text tag = new Text();
    private Set<String> stopWords = new HashSet();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Optional.ofNullable(context.getLocalCacheFiles())
                .map(Arrays::stream)
                .ifPresent(paths -> paths.forEach(this::readFile));
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (isNotHeader(key.get())) {
            String[] params = line.split("\\s+");
            String[] tags = params[1].toUpperCase().split(",");
            for (String currentTag : tags) {
                if (!stopWords.contains(currentTag)) {
                    tag.set(currentTag);
                    context.write(tag, one);
                }
            }
        }
    }

    private void readFile(Path filePath) {
        try (Stream<String> stopWordsStr = Files.lines(java.nio.file.Paths.get(filePath.getName()))) {
            stopWords = stopWordsStr.collect(Collectors.toSet());
        } catch (IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }

    public final boolean isNotHeader(Long offset) {
        return offset != 0;
    }

}
