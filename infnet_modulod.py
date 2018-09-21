from flask import Flask , abort , request
from flask_restplus import Resource , Api
import json
import buscar
import redisDatabase

app = Flask(__name__)
redisConn = redisDatabase.connectRedis()

@app.route("/buscar/jaccard", methods=['POST'])
def busca_jaccard():
    if not request.json:
        abort(400)
    content = request.get_json(silent=True)       
    return json.dumps(buscar.processaBusca(content['busca'],'jaccard',redisConn))

@app.route("/buscar/cosseno", methods=['POST'])
def busca_cosseno():
    if not request.json:
        abort(400)
    content = request.get_json(silent=True)       
    return json.dumps(buscar.processaBusca(content['busca'],'cosseno',redisConn))

if __name__ == "__main__":
    app.run()

