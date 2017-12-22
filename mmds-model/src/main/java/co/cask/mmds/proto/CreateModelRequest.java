package co.cask.mmds.proto;

/**
 * Request to create a model
 */
public class CreateModelRequest {
  private final String name;
  private final String description;

  public CreateModelRequest(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Validate the request this. This is used because this is normally created through deserializing user input,
   * which may be missing fields.
   *
   * @throws BadRequestException if the request is invalid
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new BadRequestException("Must specify a name");
    }
  }
}
