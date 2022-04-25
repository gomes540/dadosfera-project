from enum import Enum

class DataPath(Enum):
    TRIPS_PATH: str = "data/trips/"
    PAYMENT_PATH: str = "data/payment/"
    VENDOR_PATH: str = "data/vendor/"
    
class DataFilter(Enum):
    PAYMENT_FILTER: str = "cash"
    PASSENGER_FILTER: int = 2
    
class DataSchema(Enum):
    TRIPS_SCHEMA: dict = {
    "pickup_datetime": "datetime64",
    "dropoff_datetime": "datetime64",
}


