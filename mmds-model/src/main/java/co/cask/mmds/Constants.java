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

package co.cask.mmds;

/**
 * Constants for ML plugins.
 */
public class Constants {

  public static final String TRAINER_PREDICTION_FIELD = "_prediction";
  public static final String FEATURES_FIELD = "_features";

  /**
   * Modeling components
   */
  public static class Component {
    public static final String FEATUREGEN = "featuregen";
    public static final String MODEL = "model";
    public static final String TARGET_INDICES = "targetindices";
  }

  /**
   * Default dataset names
   */
  public static class Dataset {
    public static final String EXPERIMENTS_META = "experiment_meta";
    public static final String MODEL_META = "experiment_model_meta";
    public static final String MODEL_COMPONENTS = "experiment_model_components";
    public static final String SPLITS = "experiment_splits";
  }
}
