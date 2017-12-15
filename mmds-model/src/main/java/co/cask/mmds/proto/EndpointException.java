package co.cask.mmds.proto;

/**
 * Indicates an exception that can be handled in an Http endpoint in a standard fashion.
 */
public abstract class EndpointException extends RuntimeException {

  public EndpointException(String message) {
    super(message);
  }

  public EndpointException(String message, Throwable cause) {
    super(message, cause);
  }

  public abstract int getCode();
}
