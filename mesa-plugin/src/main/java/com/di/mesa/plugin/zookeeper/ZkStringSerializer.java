package com.di.mesa.plugin.zookeeper;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

/**
 * Created by Davi on 17/8/16.
 */
public class ZkStringSerializer implements ZkSerializer {

    private String encodingType;

    public ZkStringSerializer(String encodingType) {
        this.encodingType = encodingType;
    }

    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        try {
            return new String(bytes, this.encodingType);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("object.toString().getBytes error", e);
        }
    }

    public byte[] serialize(Object object) throws ZkMarshallingError {
        if (object == null) {
            return null;
        }

        if (!(object instanceof String)) {
            throw new ZkMarshallingError("The input obj must be an instance of String.");
        }

        try {
            return object.toString().getBytes(this.encodingType);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("object.toString().getBytes error", e);
        }
    }
}