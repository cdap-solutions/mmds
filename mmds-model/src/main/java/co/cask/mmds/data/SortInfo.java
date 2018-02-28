package co.cask.mmds.data;

import co.cask.mmds.proto.BadRequestException;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information about sorting order and fields.
 */
public class SortInfo {
  private List<String> fields;
  private SortType sortType;

  public SortInfo(SortType sortType) {
    this.fields = new ArrayList<>();
    this.fields.add("name");
    this.sortType = sortType;
  }

  public static SortInfo parse(String sortInfo) throws BadRequestException {
    String[] splitted = sortInfo.split("\\s+");
    validate(splitted);
    return new SortInfo(SortType.valueOf(splitted[1].toUpperCase()));
  }

  public List<String> getFields() {
    return fields;
  }

  public SortType getSortType() {
    return sortType;
  }

  private static void validate(String[] sortInfo) {
    if (sortInfo.length != 2) {
      throw new BadRequestException("Sort information should only have 2 parameters sort field and type seperated by " +
                                      "+ sign");
    }

    try {
      SortType.valueOf(sortInfo[1].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Sort type must be asc or desc type only");
    }

    // TODO Add sorting for more fields
    if (!sortInfo[0].equals("name")) {
      throw new BadRequestException("Sorting only on name is supported");
    }
  }
}
