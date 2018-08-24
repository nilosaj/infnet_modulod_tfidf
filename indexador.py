import sys
from os import listdir 
from os.path import isfile,join
import redisDatabase,utils
import nltk
import math
from nltk.stem.snowball import SnowballStemmer
from string import punctuation

redisConn = redisDatabase.connectRedis()



#Lê um arquivo para indexacao
def carregaArquivo(path):
    f = open(path)
    docs = {}
    #abre conexão com Redis
    #redisConn = redisDatabase.connectRedis()

    #leitura das informacoes do XML
    ab = False
    abkey = ""
    abvalue = ""

    for line in f:
        if (ab):
            if (line[:2] == "  "):
                abvalue += line[2:]
            else:
                ab = False
                #definicao da chave que contem o conteudo do documento emncionado no XML
                dkey= abkey.strip() #identificador do documento Ex: DOC_00178
                docs[dkey] = abvalue # conteudo do documento (texto a ser tokenizado)
                #adicao do documento ao indice reverso
                adicionaDocumento(dkey,abvalue)
        else:
            if (line[:2] == "RN"):
                abkey = line[2:].rstrip()

            elif (line[:2] == "AB"):
                abvalue = line[2:]
                ab = True                
    print("Fim do carregamento do arquivo {} . Total de arquivos na base : {}".format(path , int(utils.recuperaTotalDocs())))
    

def adicionaDocumento(doc,text):
    redisConn.hmset('doc:num:'+doc,{'content':text})
    redisConn.incr('const_total_documentos')
    redisConn.sadd('const_lista_documentos',doc)

def adicionaDocumentoAoIndice(doc,text):
    #armazena o conteudo do documento no atributo content da chave do documento
    stemmer = SnowballStemmer("english")
    stop_words = set(nltk.corpus.stopwords.words('english')+list(punctuation))
    tokens = nltk.word_tokenize(text)
    stm_tokens = [stemmer.stem(word) for word in tokens if word not in stop_words]

    total_palavras_texto = len(stm_tokens)
    #armazena o numero total de palavras no atributo document_total_words na chave do documento
    redisConn.hmset("doc:num:"+doc,{'document_total_words':total_palavras_texto})
    tokensSet = set(stm_tokens)
    frequencias = nltk.FreqDist(stm_tokens)
    for token in tokensSet:
        if not token in stop_words:
            redisConn.sadd('tk:doc:list:' + token,doc)
            total_documentos_palavra = len(redisConn.smembers('tk:doc:list:' + token))
            tfidf = utils.calculaTFIDF(frequencias[token],total_palavras_texto,total_documentos_palavra,token,True)
            redisConn.zadd('tfidf:' + token, doc,tfidf)
            redisConn.hmset('doc:tfidf:'+doc,{token:tfidf})



#chaves do Redis
#   const_lista_documentos -  set com todos os numeros (RN) dos documentos
#   const_total_documentos - chave contendo numero total de documentos
#   doc:num:$RN  - chave contendo atributos  'content' (texto do documento na integra) e 'document_total_words' (numero total de palavras no documento)
#   tk:doc:list:$token - set contendo todos os tokens presentes no documento
#   tfidf:$token - set ordenado que contem o valor de tfidf do documento no qual a palavra esta contida
#   doc:tfidf:$doc - chave contendo atributos referentes ao token cujo valor é o tfidf do token



#bloco principal
diretorio = sys.argv[1]
arquivos = [join(diretorio, arquivo) for arquivo in listdir(diretorio) if isfile(join(diretorio, arquivo))]

for arq in  arquivos:
    carregaArquivo(arq)

print("indexando ...")
for doc in redisConn.smembers('const_lista_documentos'):
    docContent = redisConn.hmget('doc:num:'+doc.decode('UTF-8'),'content')    
    adicionaDocumentoAoIndice(doc.decode('UTF-8'),docContent[0].decode('UTF-8'))
print('Fim')