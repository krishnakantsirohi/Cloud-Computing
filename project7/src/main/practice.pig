courses = Load 'hdfs://localhost:9000/courses.txt' using PigStorage(',') as (code:chararray, title:chararray);
enrollment = Load 'hdfs://localhost:9000/enrollment.txt' using PigStorage(',') as (name:chararray, code:chararray, marks:int);
j = join courses by code, enrollment by code;
i = foreach j generate courses::code as code, enrollment::marks as marks;
g = group i by code;
r = foreach g generate i.code as code, AVG(i.marks) as marks;
dump r;