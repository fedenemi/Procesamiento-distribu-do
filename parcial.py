# Se necesita conocer con que genero de juego se relaciona mas cada genero de franquicia.
# Para ello, realice una consulta que devuelva, pora cada genero de franquicia, el genero de juego con el que mas veces se lo asocio.
# Utilizar unicamente map, flatmap, reduce, reducebykey y join.

gens_f =  franquicia.flatmap(lambda x: [(x, w) for w in x[4].split()])
gens_j = games.flatmap(lambda x: [(x[2], w) for w in x[-1].split()])
joined = gens_f.join(gens_j).map(lambda x: (x[1][0], x[1][1], 1))
gens_cant = joined.reduceByKey(lambda x, y: x + y).map(lambda x: (x[0][0], (x[0][1], x[1])))
gens_cant.reduce(lambda x, y: x if x[1] > y[1] else y)


    
