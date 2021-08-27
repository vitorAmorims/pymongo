import pymongo

'''
Exercícios:
1.Retorne o nome da rota realizadas em que houve eventos de “overspeed”
2.Retorne o id dos caminhões e dos motoristas e o ssn dos motoristas com evento de “overspeed”3.Faça uma consulta que exiba todos os eventos cometidos pelo motorista diferente de “Normal”. Mostre as informações com “ID”, “name” e“ssn”dos motoristas.
'''

# mongoimport -d drivers -c timesheet --type csv --headerline --file drivers.csv
# mongoimport -d drivers -c timesheet --type csv --headerline --file timesheet.csv
# mongoimport -d drivers -c truck --type csv --headerline --file truck_event_text_partition.csv

client = pymongo.MongoClient("localhost", 27017)

db = client.drivers

drivers = db.driver.find()
# for i in drivers:
#     print(i)
# {'_id': ObjectId('6128ea8678cfbf02ae508940'), 'driverId': 43, 'name': 'Dave Patton', 'ssn': 977706052, 'location': '3028 A- St.', 'certified': 'Y', 'wage-plan': 'hours'}

timesheet = db.timesheet.find()
# for i in timesheet:
#     print(i)
# {'_id': ObjectId('6128f2631b4a1aaaed2056e1'), 'driverId': 43, 'week': 49, 'hours-logged': 56, 'miles-logged': 2743}

truck = db.truck.find()
list_eventType_truck = db.truck.distinct('eventType')
# [
# 	"Lane Departure",
# 	"Normal",
# 	"Overspeed",
# 	"Unsafe following distance",
# 	"Unsafe tail distance"
# ]
# for i in truck:
#     print(i)
# {'_id': ObjectId('6128f2d01fc9b1a8a68e906d'), 'driverId': 18, 'truckId': 49, 'eventTime': '12:23.7', 'eventType': 'Normal', 'longitude': -90.52, 'latitude': 39.71, 'eventKey': '18|49|9223370571956432141', 'CorrelationId': 1000, 'driverName': 'Grant Liu', 'routeId': 1565885487, 'routeName': 'Springfield to KC Via Hanibal', 'eventDate': '2016-06-02-20'}

# 1.Retorne o nome da rota realizadas em que houve eventos de “overspeed”
routes = db.truck.find({'eventType': "Overspeed"}, {'routeName': 1, '_id': 0})

# 2.Retorne o id dos caminhões e dos motoristas e o ssn dos motoristas com evento de “overspeed”
# db.truck.find({'eventType': "Overspeed"})

id_driver_truck_ssn = db.truck.aggregate(
    [
        { 
            $match:
                {'eventType': "Overspeed"} 
        },
        {
            $lookup:
                {
                    from: "driver",
                    localField: "driverId",
                    foreignField: "driverId",
                    as: "driver"
                }
        },
        { "$unwind": "$driver" },
        { "$project": { 
            "driverId": 1,
            "truckId": 1,
            "ssn": "$driver.ssn",
            "_id": 0
        }}
    ]
)

# 3.Faça uma consulta que exiba todos os eventos cometidos pelo motorista diferente de “Normal”. Mostre as informações com “ID”, “name” e“ssn”dos motoristas.
# db.truck.find( {eventType: {$nin: ["Normal"] } } )

look_idDriver_name_ssn_events_not_normal = db.truck.aggregate(
    [
        { 
            $match:
                {eventType: {$nin: ["Normal"] } }
        },
        {
            $lookup:
                {
                    from: "driver",
                    localField: "driverId",
                    foreignField: "driverId",
                    as: "driver"
                }
        },
        { "$unwind": "$driver" },
        { "$project": { 
            "driverId": 1,
            "driverName": 1,
            "ssn": "$driver.ssn",
            "_id": 0
        }}
    ]
)
