# Description

There are 1 master and N workers. Master receives a queue of works and distribute work evenly to given number of workers. 
Workers should repeatedly ask the master which file they should work on (until they learn there is no work left to do). 
They should perform wordcount and output results to a file. When all input files have been processed, the master should 
inform the workers that all work has concluded; upon receiving this message, the workers should exit. The master will 
then read all of the files produced by the workers, accumulates the results, and output them. The master ensures 
that all N workers are up by pinging them periodically with a heartbeat message every 3 seconds. If a worker stops 
responding, then the master must spawn another worker to continue the left-over work of the death worker. 

# Build

Compile and test
```
./gradlew build
```

Compile only
```
./gradlew assemble
```

Test
```
./gradlew test
```

For those who use Windows, you should run `gradlew.bat` with the same parameters.

Both IDEA and Eclipse have plugins for Gradle.

Some existing tests need Java 8.


# Code location

`src/main/java` is for word count code.

`src/test/java` is for tests. And `src/test/resources` is for test data.



