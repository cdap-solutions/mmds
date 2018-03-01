package co.cask.mmds.manager;

import co.cask.mmds.proto.BadRequestException;

/**
 * Holds an id
 */
public class Id {
  private String id;

  public Id(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void validate() {
    if (id == null) {
      throw new BadRequestException("Id must be specified.");
    }
  }
}
