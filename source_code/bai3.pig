reviews = LOAD '/TranAnhDuy/23520389/Lab2/hotel-review.csv' USING PigStorage(';') 
          AS (id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

pos_reviews = FILTER reviews BY sentiment == 'positive';
neg_reviews = FILTER reviews BY sentiment == 'negative';

group_pos = GROUP pos_reviews BY aspect;
count_pos = FOREACH group_pos GENERATE group AS aspect, COUNT(pos_reviews) AS total;
sorted_pos = ORDER count_pos BY total DESC;

group_neg = GROUP neg_reviews BY aspect;
count_neg = FOREACH group_neg GENERATE group AS aspect, COUNT(neg_reviews) AS total;
sorted_neg = ORDER count_neg BY total DESC;

STORE sorted_pos INTO '/TranAnhDuy/23520389/Lab2/output_bai3_positive' USING PigStorage(',');
STORE sorted_neg INTO '/TranAnhDuy/23520389/Lab2/output_bai3_negative' USING PigStorage(',');