protomol_local_map_files = {}


def put_cfg_entry(file_path):
    with open(file_path, "rb") as fp:
        protomol_local_map_files[file_path] = fp.read()


def get_cfg_file(file_path):
   return protomol_local_map_files[file_path]