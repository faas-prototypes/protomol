import logging
from lithops.storage.cloud_proxy import os as cloud_os
from lithops.storage.cloud_proxy import open as cloud_open

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
    for root, dirs, files in cloud_os.walk(prefix, topdown=True):
        for name in files:
            cloud_os.remove(cloud_os.path.join(root, name))
        for name in dirs:
            clean_remote_storage(cloud_os.path.join(root, name))