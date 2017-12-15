package co.cask.mmds.proto;

/**
 * Indicates an entity was not found
 */
public class BadRequestException extends EndpointException {

  public BadRequestException(String message) {
    super(message);
  }

  public BadRequestException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getCode() {
    return 400;
  }
}
