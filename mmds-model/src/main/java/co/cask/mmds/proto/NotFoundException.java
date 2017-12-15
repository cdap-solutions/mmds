package co.cask.mmds.proto;

/**
 * Indicates an entity was not found
 */
public class NotFoundException extends EndpointException {

  public NotFoundException(String message) {
    super(message);
  }

  public NotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public int getCode() {
    return 404;
  }
}
