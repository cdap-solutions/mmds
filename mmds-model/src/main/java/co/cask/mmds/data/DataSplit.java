package co.cask.mmds.data;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.mmds.splitter.DatasetSplitter;
import co.cask.mmds.splitter.Splitters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Information about a data split
 */
public class DataSplit {
  private final String description;
  private final String type;
  private final Map<String, String> params;
  private final List<String> directives;
  private final Schema schema;

  public DataSplit(String description, String type, Map<String, String> params,
                   List<String> directives, Schema schema) {
    this.description = description;
    this.type = type;
    this.params = params;
    this.directives = directives;
    this.schema = schema;
  }

  public String getDescription() {
    return description == null ? "" : description;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getParams() {
    return Collections.unmodifiableMap(params == null ? new HashMap<>() : params);
  }

  public List<String> getDirectives() {
    return Collections.unmodifiableList(directives == null ? new ArrayList<>() : directives);
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataSplit that = (DataSplit) o;

    return Objects.equals(description, that.description) &&
      Objects.equals(type, that.type) &&
      Objects.equals(params, that.params) &&
      Objects.equals(directives, that.directives) &&
      Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(description, type, params, directives, schema);
  }

  /**
   * validates that this is a valid split. This object is sometimes created through Gson deserialization of user input,
   * which means required fields may be missing.
   */
  public void validate() {
    if (type == null) {
      throw new IllegalArgumentException("A type must be specified.");
    }
    if (schema == null) {
      throw new IllegalArgumentException("A schema must be specified.");
    }
    DatasetSplitter splitter = Splitters.getSplitter(type);
    if (splitter == null) {
      throw new IllegalArgumentException("No splitter of type " + type + " exists.");
    }
    splitter.getParams(getParams());
  }

  /**
   * @return builder to create a DataSplit.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds DataSplits.
   */
  public static class Builder<T extends Builder> {
    protected String description;
    protected String type;
    protected Map<String, String> params;
    protected List<String> directives;
    protected Schema schema;

    public Builder() {
      description = "";
      params = new HashMap<>();
      directives = new ArrayList<>();
    }

    public T setDescription(String description) {
      this.description = description;
      return (T) this;
    }

    public T setType(String type) {
      this.type = type;
      return (T) this;
    }

    public T setParams(Map<String, String> params) {
      this.params.clear();
      this.params.putAll(params);
      return (T) this;
    }

    public T setDirectives(List<String> directives) {
      this.directives.clear();
      this.directives.addAll(directives);
      return (T) this;
    }

    public T setSchema(Schema schema) {
      this.schema = schema;
      return (T) this;
    }

    public DataSplit build() {
      DataSplit split = new DataSplit(description, type, params, directives, schema);
      split.validate();
      return split;
    }
  }
}
