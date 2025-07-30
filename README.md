-- Cargar archivo CSV desde HDFS (asegúrate que ya esté en /user/cloudera/)
raw_data = LOAD 'comentario.csv' USING PigStorage(',')
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- Extraer los comentarios
comentarios = FOREACH raw_data GENERATE comentario;

-- Tokenizar palabras de cada comentario
palabras = FOREACH comentarios GENERATE FLATTEN(TOKENIZE(LOWER(comentario))) AS palabra;

-- Filtrar palabras vacías o nulas
palabras_limpias = FILTER palabras BY palabra IS NOT NULL AND TRIM(palabra) != '';

-- Agrupar por palabra y contar ocurrencias
agrupado = GROUP palabras_limpias BY palabra;
conteo = FOREACH agrupado GENERATE group AS palabra, COUNT(palabras_limpias) AS cantidad;

-- Ordenar por cantidad descendente
ordenado = ORDER conteo BY cantidad DESC;

-- Obtener las 3 palabras más repetidas
top3 = LIMIT ordenado 3;

-- Mostrar resultados
DUMP top3;

