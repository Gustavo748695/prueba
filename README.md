-- 1. Cargar el archivo CSV
raw_data = LOAD 'comentarios.csv' USING PigStorage(',') 
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- 2. Extraer solo los comentarios
comentarios = FOREACH raw_data GENERATE comentario;

-- 3. Separar palabras en cada comentario (tokenización)
palabras = FOREACH comentarios GENERATE FLATTEN(TOKENIZE(LOWER(comentario))) AS palabra;

-- 4. Filtrar palabras vacías o nulas (opcional pero recomendable)
palabras_filtradas = FILTER palabras BY palabra IS NOT NULL AND TRIM(palabra) != '';

-- 5. Contar frecuencia de cada palabra
agrupado = GROUP palabras_filtradas BY palabra;
conteo = FOREACH agrupado GENERATE group AS palabra, COUNT(palabras_filtradas) AS total;

-- 6. Ordenar por frecuencia descendente
ordenado = ORDER conteo BY total DESC;

-- 7. Seleccionar las 3 palabras más frecuentes
top3 = LIMIT ordenado 3;

-- 8. Mostrar resultado
DUMP top3;
