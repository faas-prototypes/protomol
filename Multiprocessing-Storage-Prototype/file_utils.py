import logging
from cloudbutton.cloud_proxy import os as cloud_os
from cloudbutton.cloud_proxy import open as cloud_open

logger = logging.getLogger('pywren-protomol')


def upload_to_remote_storage(src, target_key):

    logger.info('Copying from {} to {}'.format(src, target_key))
    with open(src, 'rb') as sourceFile:
        with cloud_open(target_key,'wb') as targetFile:
            while 1:
                buf = sourceFile.read(16*1024)
                if not buf:
                    break
                targetFile.write(buf)

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





