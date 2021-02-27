from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import config
import psycopg2
import psycopg2.extras
import pandas as pd
analyzer = SentimentIntensityAnalyzer()

connection = psycopg2.connect(
    host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("""
    SELECT * FROM mention
    WHERE dt > '2021-02-26'
""")
threads = cursor.fetchall()

cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()

stocks = {}
for row in rows:
    stocks['$' + row['symbol']] = row['id']

results = []
positive_stocks = 0
for thread in threads:
    print(thread)
    title = thread[2]
    words = title.split()
    cashtags = list(
        set(filter(lambda word: word.lower().startswith('$'), words)))
    #clean up duplicates from cashtaglist
    cashtags = list(set(cashtags))
    for cashtag in cashtags:
        if cashtag in stocks:
            polarity_scores = analyzer.polarity_scores(title)
            #we assume titles to be 'positive' when polarity score is above 0.5
            if polarity_scores.get('compound') > 0.5:
                polarity_scores['title'] = title
                polarity_scores['stocks'] = cashtag
                results.append(polarity_scores)

                positive_stocks += 1

df = pd.DataFrame.from_records(results).drop_duplicates()
print(df['stocks'].value_counts())
print('# analyzed Threads: ', len(threads))
print('# positive mentioned stock tags:', positive_stocks)
