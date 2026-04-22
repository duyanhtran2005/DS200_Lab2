raw_data = LOAD '/TranAnhDuy/23520389/Lab2/hotel-review.csv' USING PigStorage(';') 
          AS (id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '/TranAnhDuy/23520389/Lab2/stopwords.txt' AS (stopword:chararray);
stopwords_clean = FOREACH stopwords GENERATE TRIM(LOWER(stopword)) AS stopword;

tokenized = FOREACH raw_data GENERATE 
    category, sentiment, 
    FLATTEN(STRSPLITTOBAG(LOWER(review), '\\s+')) AS word;

cleaned_tokens = FOREACH tokenized GENERATE 
    category, sentiment, 
    REPLACE(word, '[\\p{Punct}]', '') AS word;

cleaned_tokens = FILTER cleaned_tokens BY word != '' AND word IS NOT NULL;

joined_data = JOIN cleaned_tokens BY word LEFT OUTER, stopwords_clean BY stopword;
filtered_data = FILTER joined_data BY stopwords_clean::stopword IS NULL;

pos_data = FILTER filtered_data BY sentiment == 'positive';
neg_data = FILTER filtered_data BY sentiment == 'negative';

group_pos = GROUP pos_data BY (category, word);
count_pos = FOREACH group_pos GENERATE 
    FLATTEN(group) AS (category, word), COUNT(pos_data) AS cnt;

group_neg = GROUP neg_data BY (category, word);
count_neg = FOREACH group_neg GENERATE 
    FLATTEN(group) AS (category, word), COUNT(neg_data) AS cnt;

ordered_pos = ORDER count_pos BY category ASC, cnt DESC;
ordered_neg = ORDER count_neg BY category ASC, cnt DESC;

STORE ordered_pos INTO '/TranAnhDuy/23520389/Lab2/output_bai4_positive' USING PigStorage(',');
STORE ordered_neg INTO '/TranAnhDuy/23520389/Lab2/output_bai4_negative' USING PigStorage(',');