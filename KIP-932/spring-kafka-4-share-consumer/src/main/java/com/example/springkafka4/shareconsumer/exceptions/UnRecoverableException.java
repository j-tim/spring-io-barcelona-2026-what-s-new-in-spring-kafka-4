package com.example.springkafka4.shareconsumer.exceptions;

public class UnRecoverableException extends RuntimeException {

  public UnRecoverableException(String message) {
    super(message);
  }

}
