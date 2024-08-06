import pathlib
from datetime import datetime
import numpy as np
from compression import *
import json
import glob
import bson
import hashlib

class Zsession:
    def __init__(self, path=None, write=False):
        self.write=write
        self.session_metadata = dict()
        self.session_metadata['chunks'] = list()
        self.path = pathlib.Path(path) if path is not None else None


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.write:
            with open(str(self.path / "metadata.json"), 'w') as f:
                json.dump(self.session_metadata, f, indent=4)

    @staticmethod
    def new(path,session_metadata=None,exist_ok=False):
        zses = Zsession(path,write=True)
        if isinstance(session_metadata,dict):
            zses.session_metadata = session_metadata
        zses.session_metadata['creation_date'] = str(datetime.now())
        zses.session_metadata['creation_author'] = str('name')
        zses.path.mkdir(parents=True, exist_ok=exist_ok)
        return zses


    @staticmethod
    def open(path,write=False):
        if not pathlib.Path(path).exists():
            raise Exception("Session does not exist!")

        zses = Zsession(path,write=write)
        with open(str(zses.path / "metadata.json"), 'r') as f:
            zses.session_metadata = json.load(f)
        return zses

    def new_chunk(self,segment_name,channel_metadata,data,exist_ok=False):
        if self.write is False:
            raise Exception("Session is not writable!")
        segment_pth = self.path / str(segment_name)
        segment_pth.mkdir(parents=True,exist_ok=exist_ok)

        if not isinstance(channel_metadata,dict):
            raise Exception("")
        if 'name' not in channel_metadata:
            raise Exception("Name is required!")
        if 'fsamp' not in channel_metadata:
            raise Exception("Fsamp is required!")
        if 'uutc_start' not in channel_metadata:
            raise Exception("Uutc_start is required!")
        channel_metadata['dtype'] = str(data.dtype)
        channel_metadata['shape'] = data.shape

        chunk_pth = segment_pth / f"{channel_metadata['name']}.zdat"

        if not exist_ok and chunk_pth.exists():
            raise Exception(f"Chunk already exists: {chunk_pth}!")

        channel_metadata['compressed_data'], channel_metadata['original_md5'], channel_metadata['compressed_md5'] = compress_array(data)
        with open(str(chunk_pth), 'wb') as f:
            f.write(bson.dumps(channel_metadata))
        stop = 1



    def iter_chunks(self,hash_check=False):
        for chunk in glob.glob(str(self.path / "**/*.zdat"),recursive=True):
            with open(chunk, 'rb') as f:
                data = f.read()
                data = bson.loads(data)
                data['data'] = decompress_array(data['compressed_data'])
                del data['compressed_data']
                if hash_check:
                    if hashlib.md5(data['data']).hexdigest() != data['original_md5']:
                        raise Exception("MD5 hash check failed!")
            yield data


if __name__ == "__main__":

    with Zsession.new("test_session.zses",exist_ok=True) as zses:
        for segment_name in ['segment-001','segment-002']:
            for ch_name in ['ch1','ch2','ch3']:
                random_data = np.array(np.around(np.random.randn(60*5000*30)),dtype=np.int64)
                zses.new_chunk(segment_name=segment_name,
                               channel_metadata={'name':ch_name,
                                                 'fsamp':5000,
                                                 'uutc_start':0,
                                                 'uutc_end':1000000},
                               data=random_data,
                               exist_ok=True)





    zses = Zsession.open("test_session.zses")
    for data in zses.iter_chunks(hash_check=True):
        stop = 1