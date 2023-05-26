-- Obtener la lista de archivos en la ruta
files = LOAD '/content/flume/resultado/events-*.tmp' USING PigStorage(',') AS (filename: chararray);

-- Procesar secuencialmente cada archivo
processed_data = FOREACH files {
    -- Cargar el archivo actual
    data = LOAD Tweets.csv USING PigStorage(',') AS (
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

    -- Prueba de variables
    prueba = $HADOOP_HOME;
    
    -- Retorna los resultados
    GENERATE sorted_data;
}

-- Almacena los resultados
STORE processed_data INTO '/content/resultadoPig' USING PigStorage(',');
STORE prueba INTO '/content/resultadoPig' USING PigStorage(',');
