import logging
from lithops.multiprocessing.cloud_proxy import os
from lithops.multiprocessing.cloud_proxy import open as cloud_open

logger = logging.getLogger('pywren-protomol')


def upload_to_remote_storage(src, target_key):

    logger.info('Copying from {} to {}'.format(src, target_key))
    copied_file = open(src,'rb').read()
    with cloud_open(target_key,'wb') as targetFile:
          targetFile.write(copied_file)

    logger.info('Copy completed for {}'.format(target_key))

def read_from_remote_storage(filename):
    with cloud_open(filename, 'rb') as f:
        lines = f.readlines()
    return lines

def write_file_locally(dir, content):
    with open(dir, 'wb') as localfile:
            localfile.writelines(content)


def clean_remote_storage(prefix):
    print("clean remote storage for {}".format(prefix))
    storage = os.__getattribute__('_storage').storage
    for root, dirs, files in walk(storage,prefix):
        for name in files:
            storage.storage_handler.delete_object("protomol-replica-east", os.path.join(root, name))
        for name in dirs:
            clean_remote_storage(os.path.join(root, name))



def walk(storage, top, topdown=True, onerror=None, followlinks=False):
    dirs = []
    files = []

    for path in storage.storage_handler.list_keys("protomol-replica-east", top):
       if path.endswith('/'):
           dirs.append(path[:-1])
       else:
           files.append(path)

       if dirs == [] and files == [] and not storage.path.exists(top):
            raise StopIteration

       elif topdown:
         yield (top, dirs, files)
         for dir in dirs:
             for result in walk(storage,'/'.join([top, dir]), topdown, onerror, followlinks):
                yield result

       else:
          for dir in dirs:
             for result in walk(storage,'/'.join([top, dir]), topdown, onerror, followlinks):
                 yield result
          yield (top, dirs, files)