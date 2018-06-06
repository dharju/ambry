package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import java.nio.ByteBuffer;


/**
 * Represents a message that consists of the delete record in version {@link MessageFormatRecord.Update_Format_V1}
 * This format is used to delete a blob
 *
 *  - - - - - - - - - - - - -
 * |     Message Header  V1  |
 *  - - - - - - - - - - - - -
 * |       blob key          |
 *  - - - - - - - - - - - - -
 * |      Delete Record      |
 *  - - - - - - - - - - - - -
 *
 */
public class DeleteMessageFormatV1InputStream extends MessageFormatInputStream {
  DeleteMessageFormatV1InputStream(StoreKey key, short accountId, short containerId, long deletionTimeMs)
      throws MessageFormatException {
    int headerSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize();
    int deleteRecordSize = MessageFormatRecord.Update_Format_V1.getRecordSize();
    buffer = ByteBuffer.allocate(headerSize + key.sizeInBytes() + deleteRecordSize);
    MessageFormatRecord.MessageHeader_Format_V1.serializeHeader(buffer, deleteRecordSize,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset, headerSize + key.sizeInBytes(),
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset,
        MessageFormatRecord.Message_Header_Invalid_Relative_Offset);
    buffer.put(key.toBytes());
    // set the message as deleted
    MessageFormatRecord.Update_Format_V1.serialize(buffer,
        new UpdateRecord(accountId, containerId, deletionTimeMs, new DeleteSubRecord()));
    messageLength = buffer.capacity();
    buffer.flip();
  }
}
