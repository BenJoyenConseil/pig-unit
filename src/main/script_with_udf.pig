REGISTER datafu.jar;
DEFINE Median datafu.pig.stats.StreamingMedian();

data = LOAD 'input' using PigStorage() as (val:int);
median = FOREACH (GROUP data ALL) GENERATE Median(data);

