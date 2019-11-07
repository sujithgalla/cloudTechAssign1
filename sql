select *

from (

  select
  row_number() over(order by viewcount desc) as ant
  ,id
  ,Score
  ,Viewcount
  ,Body


  from posts

) bse

where ant <= 50000
