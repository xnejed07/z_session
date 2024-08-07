from med_loader import MedSessionWrapper

from compression_zstd_bson import *

def iter_segments(sess, chunk_size_samples=5000*60):
    data,time,channels,fsamp = sess.get_full_matrix()
    for idx0 in range(0,data.shape[1],chunk_size_samples):
        idx1 = idx0 + chunk_size_samples
        uutc_start = time[idx0]
        if idx1 > data.shape[1]:
            idx1 = data.shape[1]-1

        uutc_end = time[idx1]
        for ch in range(data.shape[0]):
            data_chunk = data[ch,idx0:idx1]
            name = channels[ch]
            yield name,data_chunk,fsamp,uutc_start,uutc_end

if __name__ == "__main__":
    file = "/Volumes/d00/d05/nejedly/tmp/sub-032_ses-001_task-rest_run-01_ieeg.medd"
    sess = MedSessionWrapper(file, "bemena")


    with Zsession.new("sub-032_ses-001_task-rest_run-01_ieeg.zses",exist_ok=True) as zses:
        for name, data, fsamp, uutc_start, uutc_end in iter_segments(sess):
            data = data.astype(np.int64)
            zses.new_chunk(segment_name=uutc_start,
                           channel_metadata={'name':name,
                                             'fsamp':int(fsamp),
                                             'uutc_start':int(uutc_start),
                                             'uutc_end':int(uutc_end)},
                           data=data,
                           exist_ok=True)