from pymongo import MongoClient
# from dotenv import dotenv_values

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')

def store_raw_data(raw_data):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db['raw_data']
        for datum in raw_data:
            collection.insert_one(datum)
        return True
    except Exception as e:
        print(str(e))
    return False

def store_decoded(data):
    pass