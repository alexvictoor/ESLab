package com.github.eslab;


import java.util.Collection;

import static org.fusesource.leveldbjni.JniDBFactory.factory;


public class LdbJniStore extends LdbStoreSupport {

    public LdbJniStore(String path, Collection<Serializer> serializers) {
        super(path, serializers, factory);
    }
}
