Java program to perform equijoin using Hadoop framework.

There are two inputs, path of input file and path of output folder. 
These both inputs are passed as an argument of the program. 

Approach: 
Driver class:
Job configuration to run map reduce program. Configured require mapper, reducer class and input, output format of the job.

Mapper class:
The mapper class takes input file and reads line of line and set up key value pairs for the next phase of job. 
The key here is the joining column and value is the whole tuple.

Reducer class:
The reducer class takes input of key, value pairs.
I have created the HashMap and stored table name as key and for value I have created list to store all tuple values correspond to join column. 
Then I have iterated over map and join all the list value correspond to join column and stored in the output file. 
This approach ensures that no matter how many table are there to join, it will generate unique tuple.

To run on window:
Please make sure Hadoop is installed in your system and configured properly. 
Necessary dependencies are added in pom file.
Hadoop version used: 2.7.7
To run program please configured below command as per your Hadoop setup:
Command: hadoop jar equijoin.jar com.aakash.hadoop.equijoin  \akash\input.txt \akash\op1 ---> hadoop jar <jar file path> <package to main class>  <input file path location> <output directory>