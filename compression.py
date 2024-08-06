import zstandard as zstd
import numpy as np
import hashlib


def compress_array(array):
    cctx = zstd.ZstdCompressor(level=3)
    array_bytes = array.tobytes()
    original_hash = hashlib.md5(array_bytes).hexdigest()
    compressed = cctx.compress(array_bytes)
    compressed_hash = hashlib.md5(compressed).hexdigest()
    return compressed,original_hash,compressed_hash


def decompress_array(compressed, dtype=np.int64, shape=-1):
    dctx = zstd.ZstdDecompressor()
    decompressed_bytes = dctx.decompress(compressed)
    decompressed_array = np.frombuffer(decompressed_bytes, dtype=dtype).reshape(shape)
    return decompressed_array