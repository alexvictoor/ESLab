package com.github.eslab;


import java.util.Collection;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class LdbStore extends LdbStoreSupport {

    public LdbStore(String path, Collection<Serializer> serializers) {
        super(path, serializers, factory);
    }
}
