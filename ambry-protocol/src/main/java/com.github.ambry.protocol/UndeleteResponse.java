/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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