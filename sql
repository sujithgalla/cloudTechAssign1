
--BELOW CODE ASSIGNS A ROW NUMBER VALUE BASED ON THE ORDER OF VIEWS OF A POST AND THEN I USE THE WHERE CLAUSE TO SWITCH BETWEEN THE VALUES TO
-- GET 4 FILES
select *
from (
  select
  row_number() over(order by viewcount desc) as ant
  ,bse.*

  from posts bse
) bse
where ant <= 50000
--where ant between 50001 and 100000
--where ant between 100001 and 150000
--where ant between 150001 and 200000

