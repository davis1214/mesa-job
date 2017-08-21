package com.di.mesa.common.exception;

/**
 * Created by Davi on 17/8/21.
 */
public class MesaException extends RuntimeException {


    public MesaException(String msg) {
        super(msg);
    }

    public MesaException(String msg, Throwable th) {
        super(msg, th);
    }

    public MesaException(Throwable th) {
        super(th);
    }

}
