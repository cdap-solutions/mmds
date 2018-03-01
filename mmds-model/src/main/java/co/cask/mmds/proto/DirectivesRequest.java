package co.cask.mmds.proto;

import java.util.List;

/**
 * Request to set model directives.
 */
public class DirectivesRequest {
  private final List<String> directives;

  public DirectivesRequest(List<String> directives) {
    this.directives = directives;
  }

  public List<String> getDirectives() {
    return directives;
  }

  /**
   * validate the request
   *
   * @throws BadRequestException if the request is invalid
   */
  public void validate() {
    if (directives == null) {
      throw new BadRequestException("Directives must be specified.");
    }
  }
}
