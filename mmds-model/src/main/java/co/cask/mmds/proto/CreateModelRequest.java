package co.cask.mmds.proto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Request to create a model
 */
public class CreateModelRequest {
  private final String name;
  private final String description;
  private final String split;
  private final List<String> directives;

  public CreateModelRequest(String name, String description, List<String> directives) {
    this(name, description, directives, null);
  }

  public CreateModelRequest(String name, String description, List<String> directives, @Nullable String split) {
    this.name = name;
    this.description = description;
    this.directives = Collections.unmodifiableList(directives);
    this.split = split;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getSplit() {
    return split;
  }

  public List<String> getDirectives() {
    return directives == null ? new ArrayList<>() : directives;
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
    if (split != null && directives != null) {
      throw new BadRequestException("Cannot specify both directives and a split.");
    }
  }
}
