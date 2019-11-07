temp  = LOAD 'QueryResults1-50k.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') as (ant:int, id:int, score:int,viewcount:int,body:chararray);
temp1 = FOREACH temp GENERATE  ant, id, viewcount, score, REPLACE(body, '<.*?.>',' ') as body;
temp2 = FOREACH temp1 GENERATE  ant, id, viewcount, score, REPLACE(body, '[^0-9a-zA-Z]+',' ');
final = FOREACH distinct_records GENERATE ant, id, viewcount, score,  REPLACE(REPLACE(REPLACE(body, '\n', ' '),'|',''),',','') AS Body;
ranked = rank final;
C = FILTER ranked BY (rank_final > 49900);
dump C;
rm cleaned_data
STORE final INTO 'cleaned_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');

[^0-9a-zA-Z]+

temp  = LOAD 'QueryResults1-50k.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') as (ant:int, id:int, score:int,viewcount:int,body:chararray);
x = FILTER temp by ($0 == 50000);
dump x;

temp  = LOAD 'QueryResults1-50k.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') as (ant:int, id:int, score:int,viewcount:int,body:chararray);
grp = GROUP temp ALL;
grp_all = FOREACH grp GENERATE COUNT(temp);
dump grp_all;