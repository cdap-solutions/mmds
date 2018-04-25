package co.cask.mmds.proto;

/**
 * Indicates an entity was not found
 */
public class ConflictException extends EndpointException {

  public ConflictException(String message) {
    super(message);
  }

  public ConflictException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getCode() {
    return 409;
  }
}
