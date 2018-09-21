import redisDatabase,utils
import nltk
import sys,math
from nltk.stem.snowball import SnowballStemmer
from string import punctuation
import json


def processaBusca(query,tipo,redis_connection):
    redisConn = redis_connection
    stemmer = SnowballStemmer("english")
    stop_words = set(nltk.corpus.stopwords.words('english')+list(punctuation))
    tokens = nltk.word_tokenize(query)
    stm_tokens = [stemmer.stem(word) for word in tokens if word not in stop_words]
    tokenSet = set(stm_tokens)
    frequencias = nltk.FreqDist(stm_tokens)
    qtfidf = {}
    dtfidf = {}
    listdoc = set({})
    lenDoc = {}
    scoreDict = {}
    print('Query executada :{}'.format(query))
    if tipo == 'cosseno':
        for t in tokenSet:
            if not t in stop_words:                
                #busca lista de documentos que contenham os tokens
                doclist = redisConn.smembers('token:'+ t+':document_list' ) 
                # imprimir documentos encontrados por token print(" token {} : {} ".format(t,doclist))
                listdoc = listdoc.union(set(doclist))
                #calcula tfidf da consulta
                qtfidf[t] =  utils.calculaTFIDF(frequencias[t] ,len(tokens) ,len(doclist) , t,'query')                  
        
        qlen =  math.sqrt(sum([value**2 for value in list(qtfidf.values()) if value != None]))
        #print('Tamanho da consulta \"{}\" : {}'.format(query,qlen))        
        for d in listdoc:            
            similaridadeDoc = utils.calculaCosSim(qtfidf,qlen,d)
            scoreDict[similaridadeDoc[0]]=similaridadeDoc[1]
            
    elif tipo == 'jaccard':
            similaridadeDoc = utils.calculaJaccard(tokenSet) 
            scoreDict=similaridadeDoc   
    else:
        print('Opcao invalida')    

    data = {}
    documents = []
    print("#######       Total de documentos encontrados : {}       #######".format(len(scoreDict)))
    data['total'] = len(scoreDict)
    for document in sorted(scoreDict.items(),key=lambda x:x[1],reverse=True):
      print("----------------------------------------------------")
      print("DOCUMENTO {} [ score: {} ] ".format(document[0],document[1]))
      print("----------------------------------------------------")
      print(redisConn.hmget('document:'+document[0]+':record_num','content'))
      doc={}
      doc['RN']= document[0]
      doc['score'] = document[1]
      doc['content'] = redisConn.hmget('document:'+document[0]+':record_num','content')
      documents.append(doc)
    data['documentos'] = documents
    return data

