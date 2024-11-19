import pymongo
import os
import pandas as pd
import pymongo.synchronous
import pymongo.synchronous.collection
import logging
from utils.exceptions import UploadException

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
        """ Based on the login data generates client assigned directly to MongoDB collection

        Args:
            db (str): name of the target database
            colection (str): name of the collection

        Returns:
            pymongo.synchronous.collection.Collection: _description_
        """
        logging.info(f'Connecting MongoDB at host: {self.host}')
        client = pymongo.MongoClient(host=self.host, port=int(self.port), username=self.username, password=self.password)
        mongo_db = client[db]
        collection = mongo_db[colection]
        return collection
        
    def upload_df(self, df:pd.DataFrame, sensor_type:str, db:str) -> None:
        """ Transfer of Pandas Dataframe content into MongoDB documents

        Args:
            df (pd.DataFrame): Pandas Dataframe with data. Each row will be transfered
                               as the MongoDB document
            sensor_type (str): type of sensor. Name will be used as MongoDB collection
            db (str): Name of MongoDB database
        """
        try:
            logging.info(f'Start upload of data. DB: {db}, collection: {sensor_type}')
            data = df.to_dict('records')
            collection_client = self.get_collection(db=db, colection=sensor_type)
            collection_client.insert_many(data)
            del collection_client
            logging.info(f'Upload complete. DB: {db}, collection: {sensor_type}')
        except Exception as e:
            raise UploadException(f'Could not upload DataFrame into Mongo. DB: {db}, collection: {sensor_type}')