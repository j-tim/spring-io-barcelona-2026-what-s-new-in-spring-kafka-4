package com.example.springkafka4.shareconsumer.exceptions;

public class RecoverableException extends RuntimeException {

  public RecoverableException(String message) {
    super(message);
  }

}
