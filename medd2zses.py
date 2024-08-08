import numpy as np

from convertors.medd.medd_wrapper import MedSessionWrapper
from z_session import Zsession

def iter_segments(sess, chunk_size_samples=5000*60):
    bi = sess.read_ts_channel_basic_info()
    channels = [k['name'] for k in bi]
    uutc_start_time = bi[0]['start_time']
    uutc_stop_time = bi[0]['end_time']
    fsamp = bi[0]['fsamp']
    for ch in channels:
        print(ch)
        data,time = sess.read_ts_channels_uutc(channel_map=[ch],uutc_map=[uutc_start_time,uutc_stop_time])
        data = np.array(data)
        for idx0 in range(0,data.shape[1],chunk_size_samples):
            idx1 = idx0 + chunk_size_samples
            uutc_start = time[idx0]
            if idx1 > data.shape[1]:
                idx1 = data.shape[1]-1

            uutc_end = time[idx1-1]
            data_chunk = data[0,idx0:idx1]
            yield ch,data_chunk,fsamp,uutc_start,uutc_end

if __name__ == "__main__":
    file = "/Users/pnejedly/Documents/iEEG/sub-032_ses-001_task-rest_run-01_ieeg.medd"
    sess = MedSessionWrapper(file, "bemena")


    with Zsession.new(file.replace(".medd",".zses"),exist_ok=True) as zses:
        for name, data, fsamp, uutc_start, uutc_end in iter_segments(sess):
            data = data.astype(np.int64)
            zses.new_chunk(segment_name=uutc_start,
                           channel_metadata={'name':name,
                                             'fsamp':int(fsamp),
                                             'uutc_start':int(uutc_start),
                                             'uutc_end':int(uutc_end)},
                           data=data,
                           exist_ok=True)