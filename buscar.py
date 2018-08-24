import redisDatabase,utils
import nltk
import sys,math
from nltk.stem.snowball import SnowballStemmer
from string import punctuation



def processaBusca(query):
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
    for t in tokenSet:
        #busca lista de documentos que contenham os tokens
        doclist = redisConn.smembers('tk:doc:list:' + t) 
        # imprimir documentos encontrados por token print(" token {} : {} ".format(t,doclist))
        listdoc = listdoc.union(set(doclist))
        #calcula tfidf da consulta
        qtfidf[t] =  utils.calculaTFIDF(frequencias[t] ,len(tokens) ,len(doclist) , t, False)        
        tk_dtfidf = {}
        for doc in  redisConn.zrange('tfidf:'+t,0,-1,False,True):
            tk_dtfidf[doc[0].decode('UTF-8')] = (t,doc[1])
        dtfidf[t]=tk_dtfidf

    #print(qtfidf)
    qlen =  math.sqrt(sum([value**2 for value in list(qtfidf.values())]))
    #print('Tamanho da consulta \"{}\" : {}'.format(query,qlen))
    scoreDict = {}
    for d in listdoc:
        lenDoc[d.decode('UTF-8')] = utils.calculaTamanhoDocumento(d.decode('UTF-8'))
        #print('tamanho do documento  {} : {}'.format(d.decode('UTF-8'),lenDoc[d.decode('UTF-8')]))
        similaridadeDoc = utils.calculaSimilaridade(d.decode('UTF-8'),dtfidf,qtfidf,lenDoc,qlen,tipo)
        scoreDict[similaridadeDoc[0]]=similaridadeDoc[1]
        #print('similaridade do documento \"{}\" com a  consulta é : {}'.format(similaridadeDoc[0],similaridadeDoc[1]))
    
    print("#######       Total de documentos encontrados : {}       #######".format(len(scoreDict)))
    for document in sorted(scoreDict.items(),key=lambda x:x[1],reverse=True):
        print("----------------------------------------------------")
        print("DOCUMENTO {} [ score: {} ] ".format(document[0],document[1]))
        print("----------------------------------------------------")
        print(redisConn.hmget("doc:num:"+document[0],'content'))
  

if (len(sys.argv)<3):
    print("Formato de busca incorreto. favor utilizar:   python buscar.py <cosseno|jaccard> <query>   ")
else:      
    redisConn = redisDatabase.connectRedis()
    #recupera termo a ser procurado na base
    tipo = sys.argv[1]
    busca = sys.argv[2] 
    processaBusca(busca)