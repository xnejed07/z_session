import numpy as np

from dhn_med_py import MedSession


class MedSessionWrapper:
    def __init__(self, file, password):
        self.session = MedSession(file, password)
        self.file = file
        self.password = password


    def read_ts_channel_basic_info(self):
        output = []
        for ch in self.session.session_info['channels']:
            ch = ch['metadata']
            output.append({'name': ch['channel_name'],
                           'fsamp': ch['sampling_frequency'],
                           'nsamp': np.NaN,
                           'ufact': 1,
                           'unit': 'uV',  # can be loaded from med info, but so far we have uV only
                           'start_time': ch['recording_time_offset'] + ch['start_time'],
                           'end_time': ch['recording_time_offset'] + ch['end_time'],
                           'channel_description': '',
                           'timezone': 0})
        return output

    def read_ts_channels_uutc(self, channel_map, uutc_map):
        self.session.set_channel_inactive('all')
        self.session.set_channel_active(channel_map)
        self.session.set_reference_channel(channel_map[0])
        mat = self.session.get_matrix_by_time(uutc_map[0], uutc_map[1])

        time = np.linspace(uutc_map[0],
                           uutc_map[1]-1000000/mat['sampling_frequency'],
                           mat['samples'].shape[1])
        time = np.array(time, dtype=np.int64)

        output = []
        for ch in channel_map:
            idx = mat['channel_names'].index(ch)
            output.append(mat['samples'][idx, :])
        return output,time



if __name__ == "__main__":
    file = "/mnt/ieeg_raw_data/example_med_dataset/sub-032/ses-001/ieeg/sub-032_ses-001_task-music_run-01_ieeg.medd"
    sess = MedSessionWrapper(file, "bemena")
    bi = sess.read_ts_channel_basic_info()
    data = sess.read_ts_channels_uutc(channel_map=['LFO2', 'LFO1'],
                                      uutc_map=[bi[0]['start_time'], bi[0]['start_time'] + 3600 * 1000000])

    stop = 1
