import redisDatabase
import math

redisConn = redisDatabase.connectRedis()

def recuperaTotalDocs():
    totalDocs =  redisConn.get('const_total_documentos')
    if (totalDocs is None):
        redisConn.set('const_total_documentos',0)
        return 0
    else:        
        return totalDocs

def  calculaTFIDF(frequencia_no_documento ,total_palavras_texto ,total_documentos_palavra , token, flgPersist):
    if total_documentos_palavra != 0:
        idf = math.log10(int(recuperaTotalDocs())/total_documentos_palavra)
        if flgPersist:
            redisConn.set('idf:' + token,idf)
        return (frequencia_no_documento/total_palavras_texto) * idf
    else:
        return 0

def calculaTamanhoDocumento(doc):
    dtfidf = redisConn.hgetall("doc:tfidf:"+doc)
    #print(dtfidf.values().decode('UTF-8'))
    values =  [float(value.decode('UTF-8'))**2 for value in list(dtfidf.values())]
    #print(values)
    
    return math.sqrt(sum(values))

def calculaSimilaridade(doc,dictDocument , dictQuery,lenDoc , lenQuery,tipo):
    if tipo == 'cosseno' :
        return calculaCosSim(doc,dictDocument , dictQuery,lenDoc , lenQuery)
    else :
        return None    

def calculaCosSim(doc,dictDocument , dictQuery,lenDoc , lenQuery):
    soma =0
    for t in dictQuery:
        if (doc in dictDocument[t]):
            soma = soma + dictQuery[t] * dictDocument[t][doc][1]
    total = soma/(lenQuery*lenDoc[doc])
    return (doc ,total)    
        
