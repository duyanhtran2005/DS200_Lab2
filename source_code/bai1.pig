raw_data = LOAD '/TranAnhDuy/23520389/Lab2/hotel-review.csv' USING PigStorage(';') 
          AS (id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stopwords = LOAD '/TranAnhDuy/23520389/Lab2/stopwords.txt' AS (stopword:chararray);
stopwords_clean = FOREACH stopwords GENERATE TRIM(LOWER(stopword)) AS stopword;

tokenized = FOREACH raw_data GENERATE 
    category, aspect, sentiment, 
    FLATTEN(STRSPLITTOBAG(LOWER(review), '\\s+')) AS word;

cleaned_tokens = FOREACH tokenized GENERATE 
    category, aspect, sentiment, 
    REPLACE(word, '[\\p{Punct}]', '') AS word;

cleaned_tokens = FILTER cleaned_tokens BY word != '' AND word IS NOT NULL;

joined_data = JOIN cleaned_tokens BY word LEFT OUTER, stopwords_clean BY stopword;
filtered_data = FILTER joined_data BY stopwords_clean::stopword IS NULL;

clean_data = FOREACH filtered_data GENERATE 
    (chararray)cleaned_tokens::word AS word, 
    (chararray)cleaned_tokens::category AS category, 
    (chararray)cleaned_tokens::aspect AS aspect, 
    (chararray)cleaned_tokens::sentiment AS sentiment;

STORE clean_data INTO '/TranAnhDuy/23520389/Lab2/output_bai1' USING PigStorage(',');