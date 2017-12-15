package co.cask.mmds.modeler.param.spec;

import java.util.List;
import java.util.Map;

/**
 * Utility class for params.
 */
public class Params {

  private Params() {
    // private constructor for utility class
  }

  /**
   * Add the specified parameters to the specified map
   *
   * @param map map to add parameters to
   * @param params parameters to add
   * @return the map with the added parameters
   */
  public static Map<String, String> putParams(Map<String, String> map, Param... params) {
    for (Param param : params) {
      map.put(param.getName(), param.getValStr());
    }
    return map;
  }

  /**
   * Add specs for the specified parameters to the list of parameter specs
   *
   * @param specs specs to add to
   * @param params params to add specs for
   * @return list of parameter specs, with new specs added
   */
  public static List<ParamSpec> addParams(List<ParamSpec> specs, Param... params) {
    for (Param param : params) {
      specs.add(param.getSpec());
    }
    return specs;
  }
}
