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
    redisConn.hmset('document:'+doc+':record_num',{'content':text})  #armazena o texto na chave
    redisConn.incr('alldocs_qtde') # incrementa o total de documentos armazenados
    redisConn.sadd('alldocs_list',doc) #adiciona o numero do documento na chave que possui a lista de todos os documentos
#def adicionaDocumentoAoIndice(doc,text):
    #armazena o conteudo do documento no atributo content da chave do documento
    stemmer = SnowballStemmer("english")
    stop_words = set(nltk.corpus.stopwords.words('english')+list(punctuation))
    tokens = nltk.word_tokenize(text)
    stm_tokens = [stemmer.stem(word) for word in tokens if word not in stop_words] #armazena os stems
    total_palavras_documento = len(stm_tokens) #verifica o numero total de stems 
    redisConn.hmset('document:'+doc+':record_num',{'document_total_words':total_palavras_documento})  #armazena o numero total de palavras no atributo document_total_words na chave do documento
    tokensSet = set(stm_tokens) # armazena uma copia de stems excluindo duplicidades 
    frequencias = nltk.FreqDist(stm_tokens) #recupera frequencias da lista de stems    
    for token in tokensSet:
        if not token in stop_words:
            redisConn.sadd('token:'+token+':document_list'  ,doc) #cria um set de token com todos os documentos que este possui
            redisConn.sadd('document:'+doc+':token_list',token)#cria um set de doc com todos os tokens que este possui
            redisConn.zadd('document:'+doc+':token_frequency_list',token,frequencias[token])#cria um set de doc com todos os tokens que este possui
            redisConn.execute_command('ZADD', 'alldocs_token_frequency_list', 'INCR', frequencias[token], token)
            #redisConn.zadd('alldocs:token:frequency:list','INCR',token,)
            #total_documentos_palavra = len(redisConn.smembers('token:document:list:' + token)) # verifica  o total de documentos no qual a o token esta incluido
            #tfidf = utils.calculaTFIDF(frequencias[token],total_palavras_documento,total_documentos_palavra,token,doc,True) #calcula op tfidf  atual 
            #redisConn.zadd('token:document:tfidf:' + token, doc,tfidf) #armazena o tfidf do documento no  token em  um sorted set
            #redisConn.hmset('document:'+doc+':token:tfidf',{token:tfidf}) #armazena o tfidf do do token no documento  em um hash
    
#def AdicionaTFIDF():
    

#chaves do Redis
#   alldocs:list -  set com todos os numeros (RN) dos documentos
#   alldocs:qtde - chave contendo numero total de documentos
#   document:record_num:{$RN}  - chave contendo atributos  'content' (texto do documento na integra) e 'document_total_words' (numero total de palavras no documento)
#   token:document:list:{$token} - set contendo todos os tokens presentes no documento
#   token:{$token}:document:tfidf: - set ordenado que contem o valor de tfidf do documento no qual a palavra esta contida
#   document:{$RN}:token:tfidf - chave contendo atributos referentes ao token cujo valor é o tfidf do token



#bloco principal
diretorio = sys.argv[1]
arquivos = [join(diretorio, arquivo) for arquivo in listdir(diretorio) if isfile(join(diretorio, arquivo))]

for arq in  arquivos:
    carregaArquivo(arq)

documentos = redisConn.smembers('alldocs_list')
token_ocorrencias = redisConn.zrange('alldocs_token_frequency_list',0,-1,False,True)

print("Gerando TFIDF")
for doc in documentos:
    for token_freq in  redisConn.zrange('document:'+doc+':token_frequency_list',0,-1,False,True):
        total_palavras_texto = redisConn.hmget('document:'+doc+':record_num','document_total_words')        
        total_documentos_palavra = len(redisConn.smembers('token:'+token_freq[0]+':document_list'  ))        
        tfidf = utils.calculaTFIDF(token_freq[1],int(total_palavras_texto[0]),total_documentos_palavra,token_freq[0],doc)
        redisConn.zadd('token:'+token_freq[0]+':tfidf_document', doc,tfidf) #armazena o tfidf do documento no  token em  um sorted set
        redisConn.hmset('document:'+doc+':token_tfidf',{token_freq[0]:tfidf}) #armazena o tfidf do do token no documento  em um hash
print("Gerando Length")
for doc in documentos:
    redisConn.hmset('alldocs_length',{doc:utils.calculaTamanhoDocumento(doc)})
print('Fim')