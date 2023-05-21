-- Cargar datos desde el archivo tweets.csv
tweets = LOAD 'ejemplo.csv' USING PigStorage(',') AS (id:int, usuario:chararray, texto:chararray, fecha:chararray);

-- Filtrar tweets por fecha
tweets_filtrados = FILTER tweets BY fecha >= '2023-01-01';

-- Contar el número de tweets por usuario
tweets_por_usuario = FOREACH (GROUP tweets_filtrados BY usuario) GENERATE group AS usuario, COUNT(tweets_filtrados) AS num_tweets;

-- Ordenar los resultados por número de tweets de manera descendente
tweets_ordenados = ORDER tweets_por_usuario BY num_tweets DESC;

-- Mostrar los resultados
-- DUMP tweets_ordenados;
STORE tweets_ordenados INTO '/content/prueba.txt' USING PigStorage(',');

