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

package co.cask.mmds.data;

import org.apache.twill.filesystem.Location;

import java.util.Objects;

/**
 * Information required to create a data split.
 */
public class DataSplitInfo {
  private final String splitId;
  private final Experiment experiment;
  private final DataSplit dataSplit;
  private final Location splitLocation;

  public DataSplitInfo(String splitId, Experiment experiment, DataSplit dataSplit, Location splitLocation) {
    this.splitId = splitId;
    this.experiment = experiment;
    this.dataSplit = dataSplit;
    this.splitLocation = splitLocation;
  }

  public String getSplitId() {
    return splitId;
  }

  public Experiment getExperiment() {
    return experiment;
  }

  public DataSplit getDataSplit() {
    return dataSplit;
  }

  public Location getSplitLocation() {
    return splitLocation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataSplitInfo that = (DataSplitInfo) o;

    return Objects.equals(splitId, that.splitId) &&
      Objects.equals(experiment, that.experiment) &&
      Objects.equals(dataSplit, that.dataSplit) &&
      Objects.equals(splitLocation, that.splitLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitId, experiment, dataSplit, splitLocation);
  }

  @Override
  public String toString() {
    return "DataSplitInfo{" +
      "splitId='" + splitId + '\'' +
      ", experiment=" + experiment +
      ", dataSplit=" + dataSplit +
      ", splitLocation=" + splitLocation +
      '}';
  }
}
