# Homework 3 for BigData course.
Simple mapreduce jobs with testing
##How to run

### Build project
```
mvn clean package
```

### To run job, counting tags, execute next command:

```
yarn jar target/bigdata2016-minskq3-task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task3.tagscount.Main <inputFile> <outputDir> [<stopWordsFile>]
```

### To run job, counting visits and sum of binding price, execute next command:

```
yarn jar target/bigdata2016-minskq3-task3-1.0.0-jar-with-dependencies.jar com.epam.bigdata2016.minskq3.task3.visitcount.Main <inputFile> <outputDir>
```

