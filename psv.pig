hadoop fs -mkdir /sqlBase
hadoop fs -put QueryResults1-50k.csv /sqlBase/QueryResults1-50k.csv
hadoop fs -put QueryResults50k-100k.csv /sqlBase/QueryResults50k-100k.csv 
hadoop fs -put QueryResults100-150k.csv /sqlBase/QueryResults100-150k.csv 
hadoop fs -put QueryResults150-200k.csv /sqlBase/QueryResults150-200k.csv 


/* LOAD 4 CSV FILES */
temp  = LOAD '/sqlBase/*.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS(Id:int, PostTypeId:int, AcceptedAnswerId:int, ParentId:int, CreationDate:datetime, DeletionDate:datetime, Score:int, ViewCount:int, Body:chararray, OwnerUserId:int, OwnerDisplayName:chararray, LastEditorUserId:int, LastEditorDisplayName:chararray, LastEditDate:datetime, LastActivityDate:datetime, Title:chararray, Tags:chararray, AnswerCount:int, CommentCount:int, FavoriteCount:int, ClosedDate:datetime, CommunityOwnedDate:datetime);

/* remove HTML */
temp1 = FOREACH temp GENERATE  Id, ViewCount, Score, OwnerUserId, REPLACE(Body, '<.*?.>',' ') as Body;

/* remove special characters */
temp2 = FOREACH temp1 GENERATE  Id, ViewCount, Score, OwnerUserId, REPLACE(Body, '[^0-9a-zA-Z]+',' ') as Body;
temp3 = FOREACH temp2 GENERATE  Id, ViewCount, Score, OwnerUserId, REPLACE(Body, '\\n',' ') as Body;
dump temp3;

STORE temp3 INTO '/final_out_pig2' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');
