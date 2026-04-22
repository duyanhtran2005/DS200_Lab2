raw_data = LOAD '/TranAnhDuy/23520389/Lab2/hotel-review.csv' USING PigStorage(';') 
          AS (id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

tokenized = FOREACH raw_data GENERATE category, FLATTEN(STRSPLITTOBAG(LOWER(review), '\\s+')) AS word;
cleaned = FOREACH tokenized GENERATE category, REPLACE(word, '[\\p{Punct}]', '') AS word;
filtered = FILTER cleaned BY word != '' AND word IS NOT NULL;

grouped_cat = GROUP filtered BY (category, word);
count_cat_word = FOREACH grouped_cat GENERATE FLATTEN(group) AS (category, word), COUNT(filtered) AS cnt_in_cat;

grouped_all = GROUP filtered BY word;
count_all_word = FOREACH grouped_all GENERATE group AS word, COUNT(filtered) AS cnt_total;

joined = JOIN count_cat_word BY word, count_all_word BY word;
relevance = FOREACH joined GENERATE count_cat_word::category AS category, count_cat_word::word AS word, (double)count_cat_word::cnt_in_cat / count_all_word::cnt_total AS score;

filtered_score = FILTER relevance BY score >= 0.8;

ordered = ORDER filtered_score BY category ASC, score DESC;

STORE ordered INTO '/TranAnhDuy/23520389/Lab2/output_bai5' USING PigStorage(',');