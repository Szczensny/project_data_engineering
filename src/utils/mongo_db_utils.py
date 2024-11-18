import pymongo
import os
import pandas as pd
import pymongo.synchronous
import pymongo.synchronous.collection

class PyMongoUtils():
    def __init__(self, host:str=None, port:int=None, username:str=None, password:str=None) -> None:
        """ PyMongoUtils is a class that simplyfies the process of interacing with MongoDB
        Args:
            host (str, optional): Host of MongoDB Server. Can be set by env variable MONGO_HOST. Defaults to None.
            port (int, optional): Port of MongoDB Server. Can be set by env variable MONGO_PORT. Defaults to None.
            username (str, optional): Username to MongoDB. Can be set by env variable MONGO_USERNAME. Defaults to None.
            password (str, optional): Password to MongoDB. Can be set by env variable MONGO_PASSWORD. Defaults to None.

        Raises:
            ValueError: Rised when any of the credentials (host, port, username, password) was not provided.
        """
        self.host = os.getenv('MONGO_HOST') if os.getenv('MONGO_HOST') is not None else host
        self.port = os.getenv('MONGO_PORT') if os.getenv('MONGO_PORT') is not None else port
        self.username = os.getenv('MONGO_USERNAME') if os.getenv('MONGO_USERNAME') is not None else username
        self.password = os.getenv('MONGO_PASSWORD') if os.getenv('MONGO_PASSWORD') is not None else password
        
        if self.host is None or self.port is None or self.username is None or self.password is None:
            raise ValueError('Credentails are not provided')
    
    def get_collection(self, db:str, colection:str)-> pymongo.synchronous.collection.Collection:
        """_summary_

        Args:
            db (str): _description_
            colection (str): _description_

        Returns:
            pymongo.synchronous.collection.Collection: _description_
        """
        client = pymongo.MongoClient(host=self.host, port=int(self.port), username=self.username, password=self.password)
        mongo_db = client[db]
        collection = mongo_db[colection]
        return collection
        
    def upload_df(self, df:pd.DataFrame, sensor_type:str, db:str) -> None:
        """_summary_

        Args:
            df (pd.DataFrame): _description_
            sensor_type (str): _description_
            db (str): _description_
        """
        data = df.to_dict('records')
        print(data)
        collection_client = self.get_collection(db=db, colection=sensor_type)
        collection_client.insert_many(data)