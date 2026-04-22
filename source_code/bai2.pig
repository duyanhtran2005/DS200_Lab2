reviews = LOAD '/TranAnhDuy/23520389/Lab2/hotel-review.csv' USING PigStorage(';') 
          AS (id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

words = FOREACH reviews GENERATE FLATTEN(STRSPLITTOBAG(LOWER(review), '\\s+')) AS word;
words_clean = FOREACH words GENERATE REPLACE(word, '[\\p{Punct}]', '') AS word;
words_filtered = FILTER words_clean BY word != '' AND word IS NOT NULL;
group_word = GROUP words_filtered BY word;
count_word = FOREACH group_word GENERATE group AS word, COUNT(words_filtered) AS freq;
top_words = FILTER count_word BY freq > 500;

group_cat = GROUP reviews BY category;
count_cat = FOREACH group_cat GENERATE group AS category, COUNT(reviews) AS total;

group_asp = GROUP reviews BY aspect;
count_asp = FOREACH group_asp GENERATE group AS aspect, COUNT(reviews) AS total;

STORE top_words INTO '/TranAnhDuy/23520389/Lab2/output_bai2_freq' USING PigStorage(',');
STORE count_cat INTO '/TranAnhDuy/23520389/Lab2/output_bai2_cat' USING PigStorage(',');
STORE count_asp INTO '/TranAnhDuy/23520389/Lab2/output_bai2_asp' USING PigStorage(',');