import pymongo

client = pymongo.MongoClient(host='localhost', port=27017, username="root", password="example")
mydb = client['sensors_data']
my_colection = mydb['test_colection']

x = {"testdata": 1, "dupa": 3, "postgres": {"rocks": True}}

my_colection.insert_one(x)