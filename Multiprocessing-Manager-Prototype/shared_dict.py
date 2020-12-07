from lithops.multiprocessing import Manager

class SharedMap:
   __instance = None

   @staticmethod
   def getInstance():
      if SharedMap.__instance == None:
         SharedMap()
      return SharedMap.__instance

   def __init__(self):

       SharedMap.__instance = Manager().dict()

s = SharedMap()

def save_file_to_redis(key,value):
    shared_map = SharedMap.getInstance()
    shared_map[key]=value

def get_value(key):
    return SharedMap.getInstance[str(key)]

