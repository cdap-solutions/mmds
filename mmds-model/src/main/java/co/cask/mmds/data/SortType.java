package co.cask.mmds.data;

/**
 * Defines sorting type
 */
public enum SortType {
  ASC("asc"),
  DESC("desc");

  private final String sortType;

  SortType(String sortType) {
    this.sortType = sortType;
  }

  @Override
  public String toString() {
    return sortType;
  }
}
