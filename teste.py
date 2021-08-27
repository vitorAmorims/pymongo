import pymongo
import json

client = pymongo.MongoClient("localhost", 27017)

db = client.aulaMongo

json_string = '{"nome": "Somewhere Far Beyond", "dataLancamento": "1992-05-30", "duracao": "3328", "artista": {"nome": "Blind Guardian"}, "artista_id": "8"}'
album = json.loads(json_string)
db.albuns.insert_one(album)

albuns = db.albuns.find()
# for i in albuns:
#     print(i)
artists = db.artistas.find()
# for i in artists:
#     print(i)

f_artistas_musicas = open('artistas_musicas.txt', 'x')
f_artistas = open('artistas.txt', 'x')
for i in artists:
    data = db.albuns.find({"artista_id" : i['id']})
    for index in data:
        if index['artista_id'] == i['id']:
            f_artistas_musicas.write(i['nome']+"-"+index['nome']+'\n')
    f_artistas.write(i['nome']+'\n')
f_artistas.close()
