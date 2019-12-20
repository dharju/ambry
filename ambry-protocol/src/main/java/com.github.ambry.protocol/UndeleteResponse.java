package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;


public class UndeleteResponse extends Response {
  private static final short Undelete_Response_Version_V1 = 1;

  public UndeleteResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.UndeleteResponse, Undelete_Response_Version_V1, correlationId, clientId, error);
  }

  public static UndeleteResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.UndeleteResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    // ignore version for now
    return new UndeleteResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("UndeleteResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("]");
    return sb.toString();
  }
}
