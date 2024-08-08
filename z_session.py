import pathlib
from datetime import datetime
import numpy as np
from compression import *
import json
import glob
import bson
import hashlib
import unittest

class Zsession:
    def __init__(self, path=None, write=False):
        self.path = pathlib.Path(path) if path is not None else None
        self.write=write
        self.session_metadata = dict()
        self.session_metadata['channels'] = list()
        self.session_metadata['segments'] = list()
        self.session_metadata['uutc_start'] = 0
        self.session_metadata['uutc_end'] = 0


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
        zses.session_metadata['segments'] = sorted(zses.session_metadata['segments'],key=lambda x: x['uutc_start'])
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
        if 'uutc_end' not in channel_metadata:
            raise Exception("Uutc_end is required!")
        channel_metadata['dtype'] = str(data.dtype)
        channel_metadata['shape'] = data.shape

        chunk_pth = segment_pth / f"{channel_metadata['name']}.zdat"

        if not exist_ok and chunk_pth.exists():
            raise Exception(f"Chunk already exists: {chunk_pth}!")

        #self.session_metadata['chunks'].append(str(f"{segment_name}/" + f"{channel_metadata['name']}.zdat"))

        if str(channel_metadata['name']) not in [x['name'] for x in self.session_metadata['channels']]:
            self.session_metadata['channels'].append({'name':str(channel_metadata['name']),
                                                      'fsamp':int(channel_metadata['fsamp'])})
        if str(segment_name) not in [x['segment'] for x in self.session_metadata['segments']]:
            self.session_metadata['segments'].append({'segment':str(segment_name),
                                                      'uutc_start':int(channel_metadata['uutc_start']),
                                                      'uutc_end':int(channel_metadata['uutc_end'])})

        self.session_metadata['uutc_start'] = min(self.session_metadata['segments'], key=lambda x: x['uutc_start'])['uutc_start']
        self.session_metadata['uutc_end'] = max(self.session_metadata['segments'], key=lambda x: x['uutc_end'])['uutc_end']

        channel_metadata['compressed_data'], channel_metadata['original_md5'], channel_metadata['compressed_md5'] = compress_array(data)
        with open(str(chunk_pth), 'wb') as f:
            f.write(bson.dumps(channel_metadata))
        stop = 1



    def read_chunk(self,segment_name,channel_name,hash_check=False):
        chunk_pth = self.path / str(segment_name) / f"{channel_name}.zdat"
        if not chunk_pth.exists():
            raise Exception(f"Chunk does not exist: {chunk_pth}!")
        with open(str(chunk_pth), 'rb') as f:
            data = f.read()
            data = bson.loads(data)
            data['data'] = decompress_array(data['compressed_data'])
            del data['compressed_data']
        if hash_check:
            if hashlib.md5(data['data']).hexdigest() != data['original_md5']:
                raise Exception("MD5 hash check failed!")
        return data

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

    def read_ts_channel_basic_info(self):
        output = []
        for ch in self.session_metadata['channels']:
            output.append({'name': ch['name'],
                           'fsamp': ch['fsamp'],
                           'nsamp': np.nan,
                           'ufact': 1,
                           'unit': 'raw',
                           'start_time': self.session_metadata['uutc_start'],
                           'end_time': self.session_metadata['uutc_end'],
                           'channel_description': '',
                           'timezone': 0})

        return output


    def read_ts_channels_uutc(self,channel_map, uutc_map):
        output = []
        for ch in channel_map:
            selected_segments = []
            for seg in self.session_metadata['segments']:
                if (seg['uutc_end'] >= uutc_map[0]) and (seg['uutc_start'] <= uutc_map[1]):
                    selected_segments.append(self.read_chunk(segment_name=seg['segment'],channel_name=ch,hash_check=False))
            ts_data = np.concatenate([s['data'] for s in selected_segments])
            ts_time = np.linspace(selected_segments[0]['uutc_start'],
                                  selected_segments[-1]['uutc_end'],
                                  ts_data.shape[0])
            idx = np.where((ts_time >= uutc_map[0]) & (ts_time < uutc_map[1]))[0]
            output.append(ts_data[idx])

        return np.array(output)



class TestZsession(unittest.TestCase):
    def setUp(self):
        self.file = "test_session.zses"
        self.file = "/Users/pnejedly/Documents/iEEG/sub-032_ses-001_task-rest_run-01_ieeg.zses"

    def test_new(self):
        with Zsession.new("test_session.zses", exist_ok=True) as zses:
            for segment_name in ['segment-001', 'segment-002']:
                for ch_name in ['ch1', 'ch2', 'ch3']:
                    random_data = np.array(np.around(np.random.randn(60 * 5000 * 30)), dtype=np.int64)
                    zses.new_chunk(segment_name=segment_name,
                                   channel_metadata={'name': ch_name,
                                                     'fsamp': 5000,
                                                     'uutc_start': 0,
                                                     'uutc_end': 1000000},
                                   data=random_data,
                                   exist_ok=True)

    def test_open(self):
        zses = Zsession.open(self.file)
        for data in zses.iter_chunks(hash_check=True):
            stop = 1

    def test_read_ts_channel_basic_info(self):
        zses = Zsession.open(self.file)
        bi = zses.read_ts_channel_basic_info()
        stop = 1

    def test_read_ts_channels_uutc(self):
        zses = Zsession.open(self.file)
        bi = zses.read_ts_channel_basic_info()
        channel = bi[0]['name']
        data = zses.read_ts_channels_uutc(channel_map=[channel],
                                          uutc_map=[bi[0]['start_time'], bi[0]['end_time']])
        stop = 1

    def test_iter_chunks(self):
        zses = Zsession.open("test_session.zses")
        for data in zses.iter_chunks(hash_check=True):
            stop = 1



if __name__ == "__main__":
    unittest.main()






