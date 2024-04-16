import mongoengine as mng
from mongoengine.document import Document
from mongoengine import IntField, DateField
from datetime import datetime
import pandas as pd

mng.connect('bot',alias='dbbot')

class bot(Document):
    meta = {"db_alias": "dbbot"}
    value = IntField()
    dt = DateField()



def mongoparse(start_date:datetime,end_date:datetime,group_type:str):
    match group_type:
        case 'month':
            pipeline = [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            {"$group": {
                "_id": {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}},
                "first_dt": {"$first": "$dt"},
                "total_value": {"$sum": "$value"}
            }},
            {"$project": {
                "_id": 0,
                "dt": {"$dateToString": {"format": "%Y-%m-%dT00:00:00", "date": "$first_dt"}},
                "total_value": 1
            }}
            ]
        case 'hour':
            pipeline = [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            {"$group": {
                "_id": {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}, "day": {"$dayOfMonth": "$dt"}, "hour": {"$hour": "$dt"}},
                "first_dt": {"$first": "$dt"},
                "total_value": {"$sum": "$value"}
            }},
            {"$project": {
                "_id": 0,
                "dt": {"$dateToString": {"format": "%Y-%m-%dT%H:00:00", "date": "$first_dt"}},
                "total_value": 1
            }}
            ]
        case 'day':
            pipeline = [
            {"$match": {"dt": {"$gte": start_date, "$lte": end_date}}},
            {"$group": {
                "_id": {"year": {"$year": "$dt"}, "month": {"$month": "$dt"}, "day": {"$dayOfMonth": "$dt"}},
                "first_dt": {"$first": "$dt"},
                "total_value": {"$sum": "$value"}
            }},
            {"$project": {
                "_id": 0,
                "dt": {"$dateToString": {"format": "%Y-%m-%dT00:00:00", "date": "$first_dt"}},
                "total_value": 1
            }}
            ]
    
    return pd.DataFrame([user for user in bot.objects.aggregate(*pipeline)])