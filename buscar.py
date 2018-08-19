import redisDatabase,utils
import nltk
import sys,math

redisConn = redisDatabase.connectRedis()

#recupera termo a ser procurado na base
busca = sys.argv[1]

def processaBusca(query):
    tokens = nltk.word_tokenize(query)
    tokenSet = set(tokens)
    frequencias = nltk.FreqDist(tokens)

    qtfidf = {}
    dtfidf = {}
    listdoc = set({})
    lenDoc = {}
    for t in tokenSet:
        #busca lista de documentos que contenham os tokens
        doclist = redisConn.smembers('tk_doc_list_' + t) 
        listdoc = listdoc.union(set(doclist))
        #calcula tfidf da consulta
        qtfidf[t] =  utils.calculaTFIDF(frequencias[t] ,len(tokens) ,len(doclist) , t, False)
        #print("tfidf do token \"{}\" na consulta é {}".format(t,qtfidf[t]))
        tk_dtfidf = {}
        for doc in  redisConn.zrange('tfidf_'+t,0,-1,False,True):
            tk_dtfidf[doc[0].decode('UTF-8')] = (t,doc[1])
        dtfidf[t]=tk_dtfidf

    #print(qtfidf)
    qlen =  math.sqrt(sum([value**2 for value in list(qtfidf.values())]))
    #print('Tamanho da consulta \"{}\" : {}'.format(query,qlen))
    scoreDict = {}
    for d in listdoc:
        lenDoc[d.decode('UTF-8')] = utils.calculaTamanhoDocumento(d.decode('UTF-8'))
        #print('tamanho do documento  {} : {}'.format(d.decode('UTF-8'),lenDoc[d.decode('UTF-8')]))
        similaridadeDoc = utils.calculaCosSim(d.decode('UTF-8'),dtfidf,qtfidf,lenDoc,qlen)
        scoreDict[similaridadeDoc[0]]=similaridadeDoc[1]
        #print('similaridade do documento \"{}\" com a  consulta é : {}'.format(similaridadeDoc[0],similaridadeDoc[1]))
    
    for document in sorted(scoreDict.items(),key=lambda x:x[1],reverse=True):
        print("DOCUMENTO {} [ score: {} ] ".format(document[0],document[1]))
        print(redisConn.hmget("doc_"+document[0],'content'))

    
#resultado = redisConn.zrange('tfidf_'+token,0,-1,False,True)
#print('TOTAL DE DOCUMENTOS CONTENDO A PALAVRA {} : {}'.format(token,len(resultado)))
#for d in resultado:
    #docText = redisConn.hmget(format(d[0].decode('UTF-8'),'content'))
#    docText = redisConn.hmget(format(d[0].decode('UTF-8')),'content')
#    
#    print("# No documento {} ocorre {} vezes ".format(d[0].decode('UTF-8'), int(d[1])))
#    print("TEXTO: {}".format(docText))

processaBusca(busca)