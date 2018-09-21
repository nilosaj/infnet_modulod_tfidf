import redisDatabase
import math

redisConn = redisDatabase.connectRedis()

def recuperaTotalDocs():
    totalDocs =  redisConn.get('alldocs_qtde')
    if (totalDocs is None):
        redisConn.set('alldocs_qtde',0)
        return 0
    else:        
        return totalDocs

def  calculaTFIDF(frequencia_no_documento ,total_palavras_texto ,total_documentos_palavra , token, docNum):
    #print("palavra {} esta em {} documentos".format(token,total_documentos_palavra))
    if total_documentos_palavra != 0:        
        idf = math.log10(int(recuperaTotalDocs())/total_documentos_palavra) 
        tfidf = (frequencia_no_documento/total_palavras_texto) * idf       
        #print("frequencia do token '{}' no documento {} é {}. TFIDF calculado = {} . total de palavras no documento :{}".format(token,docNum,frequencia_no_documento,tfidf,total_palavras_texto))
        return tfidf
    
    

def calculaTamanhoDocumento(doc):
    dtfidf = redisConn.hgetall('document:'+doc+':token_tfidf')
    #print(dtfidf.values().decode('UTF-8'))
    values =  [float(value)**2 for value in list(dtfidf.values())]
    #print(values)    
    return math.sqrt(sum(values))

 

def calculaCosSim(dictQtfidf , lenQuery , doc):
    soma =0    
       
    for tk_tfidf in dictQtfidf:
        vToken = redisConn.hmget('document:'+doc+':token_tfidf',tk_tfidf)
        if (vToken[0] is not None) :
            soma += float(vToken[0]) + float(dictQtfidf[tk_tfidf])
    docLength = redisConn.hmget('alldocs_length',doc)              
    total = soma/(lenQuery * float(docLength[0]))
    #print(" a similaridade do documento {} a query é :{}".format(doc,total))
    return (doc ,total)    
        
def calculaJaccard(queryTokens):    
    docDict = {}
    for doc in redisConn.smembers('alldocs_list'):
        docTkens = set(redisConn.smembers('document:'+doc+':token_list'))
        i = docTkens.intersection(queryTokens)
        u = docTkens.union(queryTokens)
        sim = len(i)/len(u)
        #print("documento {} tem {} interseções e {} unioes e similaridade é {} ".format(doc,i,u,sim))
        if (sim > 0):
            docDict[doc] = len(i)/len(u)
        
        
    return (docDict)