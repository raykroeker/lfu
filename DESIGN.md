## Version 2
Version 1 was insufficient in its approach as it ended up generating a large number
of file chunks which BackBlaze was unable to support.  In version 2; we instead
split the origin file into a number of parts (hashing the file as we go) then copy
the file part directly to the http upload writer.

### File Part
A portion of the original file backed by temporary file.  The file part includes an 
ordering, hash, origin offset and path.

## Version 1
Version 1 of the implementation lifts chunks of bytes large enough to fit in memory
from disk, then sends them on a channel to multiple threads that then hash the
chunk and upload them to BackBlaze.

### File Chunk
A batch of bytes along with an ordering, hash and origin offset.

### File Reader
A local file reader that batches bytes into a single chunk then emits them on
a channel for a consumer.

### Log Writer
Writes file chunk meta details to a log.  Uses a separate timer to flush the underlying file.  Orders
the file chunk meta detail on read (relies on the log to fit in memory).
