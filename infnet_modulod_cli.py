import buscar,redisDatabase
import sys


if (len(sys.argv)<3):
    print("Formato de busca incorreto. favor utilizar:   python buscar.py <cosseno|jaccard> <query>   ")
else:      
    redisConn = redisDatabase.connectRedis()
    #recupera termo a ser procurado na base
    tipo = sys.argv[1]
    busca = sys.argv[2] 

buscar.processaBusca(busca,tipo,redisConn)