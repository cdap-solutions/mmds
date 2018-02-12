package co.cask.mmds.splitter;

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
