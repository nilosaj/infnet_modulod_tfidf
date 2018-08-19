import sys
from os import listdir 
from os.path import isfile,join
import redisDatabase,utils
import nltk
import math

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
    redisConn.hmset('doc_'+doc,{'content':text})
    redisConn.incr('total_documentos')
    redisConn.sadd('lista_documentos',doc)

def adicionaDocumentoAoIndice(doc,text):
    #armazena o conteudo do documento no atributo content da chave do documento
    
    stop_words = set(nltk.corpus.stopwords.words('english'))
    tokens = nltk.word_tokenize(text)
    total_palavras_texto = len(tokens)
    #armazena o numero total de palavras no atributo document_total_words na chave do documento
    redisConn.hmset("doc_"+doc,{'document_total_words':total_palavras_texto})
    tokensSet = set(tokens)
    frequencias = nltk.FreqDist(tokens)
    for token in tokensSet:
        if not token in stop_words:
            redisConn.sadd('tk_doc_list_' + token,doc)
            total_documentos_palavra = len(redisConn.smembers('tk_doc_list_' + token))
            tfidf = utils.calculaTFIDF(frequencias[token],total_palavras_texto,total_documentos_palavra,token,True)
            redisConn.zadd('tfidf_' + token, doc,tfidf)
            redisConn.hmset('doc_tfidf_'+doc,{token:tfidf})





#bloco principal
diretorio = sys.argv[1]
arquivos = [join(diretorio, arquivo) for arquivo in listdir(diretorio) if isfile(join(diretorio, arquivo))]

for arq in  arquivos:
    carregaArquivo(arq)

print("indexando ...")
for doc in redisConn.smembers('lista_documentos'):
    docContent = redisConn.hmget('doc_'+doc.decode('UTF-8'),'content')    
    adicionaDocumentoAoIndice(doc.decode('UTF-8'),docContent[0].decode('UTF-8'))
print('Fim')