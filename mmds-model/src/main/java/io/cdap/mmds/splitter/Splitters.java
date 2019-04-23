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

package io.cdap.mmds.splitter;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Collection of built-in splitters.
 */
public class Splitters {
  private static final List<DatasetSplitter> SPLITTERS = ImmutableList.of(
    new RandomDatasetSplitter());
  private static final Map<String, DatasetSplitter> SPLITTER_MAP = SPLITTERS.stream().collect(
    Collectors.toMap(splitter -> splitter.getSpec().getType(), splitter -> splitter));

  public static Collection<String> getTypes() {
    return SPLITTER_MAP.keySet();
  }

  public static Collection<DatasetSplitter> getSplitters() {
    return SPLITTERS;
  }

  @Nullable
  public static DatasetSplitter getSplitter(String type) {
    return SPLITTER_MAP.get(type);
  }

}
