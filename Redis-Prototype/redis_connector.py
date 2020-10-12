import redis

# Move this configuration to cloud storage

configuration = {

	"connector": {
		"host": "3.216.27.64",
		"port": 6379

	}
}


class RedisConnector:
   __instance = None

   @staticmethod
   def getInstance():
      if RedisConnector.__instance == None:
         RedisConnector()
      return RedisConnector.__instance

   def __init__(self):
       host = configuration["connector"]["host"]
       port = configuration["connector"]["port"]
       #password = configuration["connector"]["password"]
       #db = configuration["connector"]["db"]
       RedisConnector.__instance = redis.Redis(host=host, port=port)

s = RedisConnector()

def save_file_to_redis(key,value):
    redis_connector = RedisConnector.getInstance()
    redis_connector.set(str(key),value)

def get_value(key):
    redis_connector = RedisConnector.getInstance()
    return redis_connector.get(str(key))

def clear_db():
    redis_connector = RedisConnector.getInstance()
    redis_connector.flushdb()
