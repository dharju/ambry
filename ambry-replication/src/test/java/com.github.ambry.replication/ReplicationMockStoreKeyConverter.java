package com.github.ambry.replication;

import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyConverter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


class ReplicationMockStoreKeyConverter implements StoreKeyConverter {

    private boolean throwException = false;

    private final Map<StoreKey, StoreKey> map = new HashMap<>();

    void put(StoreKey key, StoreKey value) {
      map.put(key, value);
    }

    void setThrowException(boolean bool) {
      throwException = bool;
    }

    @Override
    public Map<StoreKey, StoreKey> convert(Collection<? extends StoreKey> collection) throws Exception {
      if (throwException) {
        throw new MockStoreKeyConverterException();
      }
      Map<StoreKey, StoreKey> answer = new HashMap<>();
      for (StoreKey storeKey : collection) {
        if (!map.containsKey(storeKey)) {
          answer.put(storeKey, storeKey);
        } else {
          answer.put(storeKey, map.get(storeKey));
        }
      }
      return answer;
    }

    /**
     * Same as Exception; just used to identify exceptions
     * thrown by the outer class
     */
    static class MockStoreKeyConverterException extends Exception {

    }
}
