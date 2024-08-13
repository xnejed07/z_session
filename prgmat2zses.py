import numpy as np

from convertors.prg_mat.PrgMat import *
from z_session import Zsession
import glob

def iter_segments(sess, chunk_size_samples=250*60):
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
    files = "/Volumes/mdpm-d00/d04/eeg_data/lfuk_mouse/397_converted/*.mat"

    outfile = "397.zses"
    with Zsession.new(outfile, exist_ok=True) as zses:

        for file in sorted(glob.glob(files)):
            try:
                print(file)
                sess = PrgMat(file)
                for name, data, fsamp, uutc_start, uutc_end in iter_segments(sess):
                    data = data.astype(np.double)
                    zses.new_chunk(segment_name=int(uutc_start),
                                   channel_metadata={'name':name,
                                                     'fsamp':int(fsamp),
                                                     'uutc_start':int(uutc_start),
                                                     'uutc_end':int(uutc_end)},
                                   data=data,
                                   exist_ok=True)
            except Exception as e:
                print(f"Error processing {file}: {e}")
                continue