import asyncio
from datetime import datetime
from dateutil.relativedelta import relativedelta
from motor.motor_asyncio import AsyncIOMotorClient
import os

async def aggregate_data(dt_from, dt_upto, group_type):
    client = AsyncIOMotorClient(os.getenv("MONGODB_URI"))
    db = client[os.getenv("DB_NAME")]
    collection = db[os.getenv("COLLECTION_NAME")]

    dt_from, dt_upto = map(datetime.fromisoformat, [dt_from, dt_upto])
    query = {"dt": {"$gte": dt_from, "$lte": dt_upto}}

    groupings = {
        "hour": ({"year": {"$year": "$dt"}, "month": {"$month": "$dt"}, "day": {"$dayOfMonth": "$dt"}, "hour": {"$hour": "$dt"}}, "%Y-%m-%dT%H:00:00", relativedelta(hours=1)),
        "day": ({"year": {"$year": "$dt"}, "month": {"$month": "$dt"}, "day": {"$dayOfMonth": "$dt"}}, "%Y-%m-%dT00:00:00", relativedelta(days=1)),
        "month": ({"year": {"$year": "$dt"}, "month": {"$month": "$dt"}}, "%Y-%m-01T00:00:00", relativedelta(months=1))
    }

    if group_type not in groupings:
        raise ValueError("Invalid group_type. Allowed values: hour, day, month")

    group, date_format, step = groupings[group_type]
    group_stage = {"_id": group, "total_payment": {"$sum": "$value"}}
    sort_stage = {f"_id.{k}": 1 for k in group.keys()}
    pipeline = [{"$match": query}, {"$group": group_stage}, {"$sort": sort_stage}]
    
    result = collection.aggregate(pipeline)

    full_range = []
    current = dt_from
    while current <= dt_upto:
        full_range.append(current)
        current += step

    aggregated_dict = {
        datetime(entry["_id"]["year"], entry["_id"].get("month", 1), entry["_id"].get("day", 1), entry["_id"].get("hour", 0)): entry["total_payment"]
        async for entry in result
    }

    dataset = [aggregated_dict.get(dt, 0) for dt in full_range]
    labels = [dt.strftime(date_format) for dt in full_range]

    return {"dataset": dataset, "labels": labels}