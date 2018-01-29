# Multiflex
Flexible dataseries storage file format based on SQLite and efficient Integer packing


## Endianess

Numeric data are stored in *little-endian* format for efficiency reasons (most current machines use this format).


## Descriptor

A descriptor holds information to encode/decode the stored data in the order at which the data appears in the chunks.

The descriptors are internally stored as binary blobs that contain multiple instances (one per data contained in the associated chunks) of the following sequence:

Offset | Name | Description
--- | --- | ---
0   | Length | 32-bit integer, Number of compressed records in this descriptor
4   | ID | First application defined numeric track identifier (32-bit signed) 
8   | Encoding | Type of data and mode of compression used (see *encoding*) for track 1 (32-bit signed) 


## Chunk format

Chunks are blobs encoded using the following format:

Name | Description
--- | ---
Indices | Compressed 32-bit integers with the byte offsets of the data fields mentioned in the *descriptor*, starting after the end of field *Data Length*
Data Length | Compressed 32-bit integers with the uncompressed length of the data fields mentioned in the *descriptor*
Data 0 | Data for stream 0 (encoded as defined by the specified *descriptor*)
Data 1 | Data for stream 1
Data n | Data for stream n


## Encoding

Implemented encodings for data stored in chunks:

Number | Name | Description
--- | --- | ---
0   | BINARY | Uncompressed binary data
1   | UTF8_STRING | Uncompressed UTF-8 string
2   | INT32_VAR_BYTE_FAST_PFOR | Array of 32-bit signed integer, compressed using VariableByte and FastPFOR
3   | INT32_DELTA_VAR_BYTE_FAST_PFOR | Array of 32-bit signed integers, compressed using Delta, VariableByte and FastPFOR
4   | INT32_CENTER31BIT_VAR_BYTE_FAST_PFOR | Array of 32-bit signed integers (excluding the number -2147483648), compressed using a arithmetic coding scheme, VariableByte and FastPFOR. This is primarily useful for embedded systems with tight memory constraints (< 64K or so).
