
import h5py
import numpy as np
import json
from scipy.io import loadmat
from datetime import datetime
class PrgMat:
    def __init__(self,file):
        self.file = file
        self.data = None
        self.metadata = None
        self.read_file()

        date_time_obj = datetime.strptime(self.metadata['start_date'], '%d-%b-%Y %H:%M:%S')

        self.metadata['uutc_start'] = int(date_time_obj.timestamp()*1000000)
        self.metadata['uutc_stop'] = int(self.metadata['uutc_start'] + self.data.shape[1]/self.metadata['fs']*1000000)-1000000/self.metadata['fs']
        self.metadata['timezone'] = 'Prague'


    def read_file(self):
        mat = loadmat(self.file)
        self.data = mat['x']

        with open(self.file.replace(".mat",".json")) as f:
                self.metadata = json.load(f)
        stop = 1

    def read_ts_channel_basic_info(self):
        output = []
        for ch in self.metadata['channels']:
            output.append({'name': ch,
                           'fsamp': self.metadata['fs'],
                           'nsamp': self.data.shape[1],
                           'ufact': 1,
                           'unit': 'unknown',
                           'start_time': int(self.metadata['uutc_start']),
                           'end_time': int(self.metadata['uutc_stop']),
                           'channel_description': '',
                           'timezone': self.metadata['timezone']})
        return output

    def read_ts_channels_uutc(self, channel_map, uutc_map):
        output = []
        time = np.linspace(self.metadata['uutc_start'],
                           self.metadata['uutc_stop'],
                           self.data.shape[1])
        idx_time = np.where((time >= uutc_map[0]) & (time <= uutc_map[1]))[0]
        for ch in channel_map:
            idx = self.metadata['channels'].index(ch)
            output.append(self.data[idx, idx_time])

        return output, time[idx_time]

if __name__ == "__main__":
    file = "/Volumes/mdpm-d00/d04/eeg_data/lfuk_mouse/397_converted/Moni ET 397-220207_160240-INTN-250HZ-001.mat"
    prg_mat = PrgMat(file)
    bi = prg_mat.read_ts_basic_info()
    print(prg_mat.metadata)
    print(prg_mat.data)