import pymongo,json,time,datetime,re
from bson import json_util,datetime
from pymongo import MongoClient
from collections import Counter

client = MongoClient()
client = MongoClient('localhost', 27017)

db = client['twixplorer'] 
tweets = db['tweets']
dias = 3

now = int(time.time())*1000
before = now - dias*24*60*60*1000
before = 0

	
queryTempo = {'status.timestamp_ms': {
	               "$gte": before
	            }}
queryAndroid = {"status.source": {
					"$regex":"Android"
					}
				}
queryIOS = {"$or":
			[
				{"status.source":{"$regex":"iPhone"}},
				{"status.source":{"$regex":"iPad"}},
				{"status.source":{"$regex":"iOS"}}
			]}
queryWindowsPhone = {"status.source": {"$regex":"Windows Phone"}}

queryWebClient = {"$or":
				[
					{"status.source":{"$regex":"Web Client"}},
					{"$and":
						[
							{"status.source":{"$regex":"Windows"}},
							{"$nor":#nor(A,A) = not(A), por algum motivo não aceita not, então usei 2 nor's
								[
									{"status.source":{"$regex":"Windows Phone"}},
									{"status.source":{"$regex":"Windows Phone"}}
								]
							}
						]
					}
				]}
queryMobileWeb = {"status.source":{"$regex":"Mobile Web"}}

queryOutros = {"$nor":
				[
					{"status.source":{"$regex":"Web Client"}},
					{"status.source":{"$regex":"Windows"}},
					{"status.source":{"$regex":"Android"}},
					{"status.source":{"$regex":"iPhone"}},
					{"status.source":{"$regex":"iPad"}},
					{"status.source":{"$regex":"iOS"}},
					{"status.source":{"$regex":"Windows Phone"}},
					{"status.source":{"$regex":"Mobile Web"}}
				]}

data = [
	{"regiao":"Norte","estados":[
		{"estado":"ac"},
		{"estado":"am"},
		{"estado":"ap"},
		{"estado":"pa"},
		{"estado":"ro"},
		{"estado":"rr"},
		{"estado":"to"}
		]},
	{"regiao":"Nordeste","estados":[
		{"estado":"al"},
		{"estado":"ba"},
		{"estado":"ce"},
		{"estado":"ma"},
		{"estado":"pb"},
		{"estado":"pe"},
		{"estado":"pi"},
		{"estado":"rn"},
		{"estado":"se"}
		]},
	{"regiao":"Centro-Oeste","estados":[
		{"estado":"go"},
		{"estado":"ms"},
		{"estado":"mt"}
		]},
	{"regiao":"Sudeste","estados":[
		{"estado":"es"},
		{"estado":"mg"},
		{"estado":"rj"},
		{"estado":"sp"}	
		]},
	{"regiao":"Sul","estados":[
		{"estado":"pr"},
		{"estado":"rs"},
		{"estado":"sc"}
		]}
]
for item in data:
	print "\t~-~--~-~-"+item["regiao"]+"~-~--~-~-"
	for estado in item['estados']:
		strEstado = estado["estado"]
		print "\n-+-"+estado["estado"].upper()+"-+-\n"

		queryEstado = {"categories":{"$regex":estado["estado"]}}

 		nAndroid = db.tweets.find({"$and":[queryTempo,queryEstado,queryAndroid]}).count()
 		nIOS = db.tweets.find({"$and":[queryTempo,queryEstado,queryIOS]}).count()
 		nWinPhone = db.tweets.find({"$and":[queryTempo,queryEstado,queryWindowsPhone]}).count()
 		nWebClient = db.tweets.find({"$and":[queryTempo,queryEstado,queryWebClient]}).count()
 		nMobWeb = db.tweets.find({"$and":[queryTempo,queryEstado,queryMobileWeb]}).count()
 		nOutros = db.tweets.find({"$and":[queryTempo,queryEstado,queryOutros]}).count()

 		print "nAndroid  \t"+str(nAndroid)
 		print "nIOS      \t"+str(nIOS)
 		print "nWinPhone \t"+str(nWinPhone)
 		print "nWebClient\t"+str(nWebClient)
 		print "nMobileWeb\t"+str(nMobWeb)
 		print "nOutros   \t"+str(nOutros)

 		print "\nA soma e :        "+str(nAndroid+nIOS+nWinPhone+nWebClient+nMobWeb+nOutros)

 		estado["tecnologias"] = {"Android":nAndroid,"iOS":nIOS,"Windows Phone":nWinPhone,"WebClient":nWebClient,"Mobile Web":nMobWeb,"Outros":nOutros}


with open("data.json", 'w') as outfile:
    json.dump(data, outfile)
