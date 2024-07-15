# Dada una colección de documentos queremos encontrar frases de 1 , 2 o 3 palabras que sean anagramas de otras.
# Por ejemplo: (“Postmaster”, “Stamp store”) o (“A telescope” , “To see Place”) o (“The cockroach”, “cook catch her”).
# Esta tarea implica una combinatoria muy difícil por lo que se decide usar Map-Reduce para paralelizarla.
# Usando Map-Reduce programar la solución a este problema listando todos los pares de anagramas entre frases de 1, 2 o 3 palabras.
# Como puede verse en los ejemplos se ignoran mayúsculas y minúsculas y los espacios en blanco, puntuación, etc.
# Suponer que existe la función word_tokenizer que recibe un texto y devuelve un vector de palabras ya convertidas a minúsculas y sin puntuación.

from pyspark import SparkContext
sc = SparkContext.getOrCreate()

letters = set(list(string.ascii_lowercase) + list(string.ascii_uppercase)\
    +list(string.digits) + [' ','.','-','\n'])
sampleText = ''.join(x for x in wp.page("Alan turing").content if x in letters).replace('\n','.').split('.')
rdd = sc.parallelize(sampleText)

# En nuestro caso usaremos cada oracion del articulo como un documento.
# Para encontrar los anagramas pondremos todos los documentos en lowercase de modo que por ejemplo "Alter the" y "the later" sean anagramas.
# Tambien eliminaremos las frases que han quedado vacias.

rdd = rdd.map(lambda y: y.lower()).filter(lambda x: len(x)>0)


# Vamos a definir una funcion que nos obtiene los n-gramas de n palabras de cada elemento de un rdd
def ngrams(element,n):
    lis = []
    splitted = element.split()
    for ind in range(0,len(splitted)-n):
        lis.append(' '.join(splitted[ind:ind+n]))
    return lis

# Obtenemos los n-gramas para n 1,2,3 y unimos todo.
# Luego de esto, tenemos una lista de listas de n-gramas, aplicamos flatmap para obtener una sola lista con los elementos de cada una de las tres listas.
rdd1 = rdd.map(lambda x: ngrams(x,1))
rdd2 = rdd.map(lambda x: ngrams(x,2))
rdd3 = rdd.map(lambda x: ngrams(x,3))
rddf = rdd1.union(rdd2.union(rdd3)).flatMap(lambda x: x)

# El enunciado nos pide encontrar los pares de anagramas en este rdd. Eso implica que tendran las mismas letras, los espacios no importan. 
# Entonces uniremos los n elementos de cada uno de los n-gramas, eliminando los espacios y ordenando el string (podrian armarse sets de este mismo modo).
# Con esto armamos una tupla de (elemento ordenado,elemento). 
# Agrupando segun la clave (el primer elemento de la tupla == el elemento ordenado) tenemos una lista de todos los anagramas por clave.
# Claramente debemos filtrar aquellos que hayan aparecido solo una vez.
rddf = rddf.map(lambda x: (''.join(sorted(x.replace(' ',''))),x))
rddf = rddf.groupByKey().map(lambda x: list(set(x[1]))).filter(lambda x: len(x)>1)

# En el output de la celda anterior podemos ver lo siguiente:
# casi todos los "anagramas" de multiples palabras son en realidad las mismas palabras ordenadas de distinto modo. Procedemos a arreglar esto:
def uniqueWords(ngrams):
    r = []
    l = []
    for i in ngrams:
        iset = set(i.split())
        if set.difference(*(r+[iset])) != set([]):
            r.append(iset)
            l.append(i)
    return l
    
rddf = rddf.map(lambda x: uniqueWords(x)).filter(lambda x: len(x)>1)
rddf.collect()