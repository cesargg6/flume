-- Cargar datos desde el archivo tweets.csv
-- /content/flume/resultado/events-*
-- tweets = LOAD '/content/HotelBookings.csv' USING PigStorage(',') AS (id:int, usuario:chararray, texto:chararray, fecha:chararray);
data = LOAD 'HotelBookings.csv' USING PigStorage(',') AS (
    tweet_id: long,
    airline_sentiment: chararray,
    airline_sentiment_confidence: float,
    negativereason: chararray,
    negativereason_confidence: float,
    airline: chararray,
    airline_sentiment_gold: chararray,
    name: chararray,
    negativereason_gold: chararray,
    retweet_count: int,
    text: chararray,
    tweet_coord: chararray,
    tweet_created: chararray,
    tweet_location: chararray,
    user_timezone: chararray
);

-- Filtra los tweets negativos
negative_tweets = FILTER data BY airline_sentiment == 'negative';

-- Agrupa los tweets por aerolínea
grouped_data = GROUP negative_tweets BY airline;

-- Calcula la cantidad de tweets negativos por aerolínea
tweet_count = FOREACH grouped_data GENERATE group AS airline, COUNT(negative_tweets) AS num_negative_tweets;

-- Ordena los resultados por cantidad de tweets negativos de forma descendente
sorted_data = ORDER tweet_count BY num_negative_tweets DESC;

-- Filtra los registros eliminando los valores NULL
filtered_data = FILTER data BY
    tweet_id IS NOT NULL AND
    airline_sentiment IS NOT NULL AND
    airline_sentiment_confidence IS NOT NULL AND
    negativereason IS NOT NULL AND
    negativereason_confidence IS NOT NULL AND
    airline IS NOT NULL AND
    airline_sentiment_gold IS NOT NULL AND
    name IS NOT NULL AND
    negativereason_gold IS NOT NULL AND
    retweet_count IS NOT NULL AND
    text IS NOT NULL AND
    tweet_coord IS NOT NULL AND
    tweet_created IS NOT NULL AND
    tweet_location IS NOT NULL AND
    user_timezone IS NOT NULL;

-- Mostrar los resultados
-- DUMP tweets_ordenados;
STORE filtered_data INTO '/content/resultadoPig' USING PigStorage(',');
