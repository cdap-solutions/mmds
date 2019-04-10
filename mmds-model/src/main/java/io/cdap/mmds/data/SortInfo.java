/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.mmds.data;

import io.cdap.mmds.proto.BadRequestException;

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
