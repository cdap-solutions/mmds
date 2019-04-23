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

/**
 * Model status. The state transition diagram is:
 *
 *       |----------------------- unassign split--------------------------------------------|
 *       |                                                                      ^           |
 *       |-- set directives --|                                                 |           |
 *       |                    |           |----------- create split --------|   |           |
 *       v       |------------|           |                                 |   |           |
 *   PREPARING --|                        v       |-- split failed --> SPLIT_FAILED         |
 *               |-- create split --> SPLITTING --|                                      |--|
 *                                        ^       |-- split succeeded --> DATA_READY ----|
 *                                        |                                 |            |-- train --> TRAINING
 *                                        |---------------------------------|
 *
 *      |--------------- train -------------|
 *      |                                   |
 *      v       |-- training failed --> TRAINING_FAILED -- unassign split --> PREPARING
 *   TRAINING --|
 *              |-- training succeeded --> TRAINED -- deploy model --> DEPLOYED
 *
 *
 *
 * A model starts out in the PREPARING state. In the PREPARING state, directives can be modified.
 *
 * From the PREPARING state, a data split can be created.
 * If the split succeeds, the model moves to DATA_READY. If the split fails, the model moves to SPLIT_FAILED.
 *
 * From the SPLIT_FAILED state, a new split can be created in case the failure was a transient failure. The split
 * can also be unassigned, which brings the model back to the PREPARING state.
 *
 * From the DATA_READY state, the split can be unassigned, which moves it to PREPARING. The model can also be
 * trained, which moves it to TRAINING.
 *
 * From TRAINING, it moves to TRAINED if the model training succeeds.
 * If the training failed, it moves to TRAINING_FAILED.
 *
 * From the TRAINED state, it can be deployed, which moves it to DEPLOYED.
 *
 * From the TRAINING_FAILED state, it can be trained again, which moves it to the TRAINING state. The split can also
 * be unassigned, which moves it back to the PREPARING state.
 */
public enum ModelStatus {
  PREPARING("Preparing"),
  SPLITTING("Splitting"),
  SPLIT_FAILED("Split Failed"),
  DATA_READY("Data Ready"),
  TRAINING("Training"),
  TRAINED("Trained"),
  TRAINING_FAILED("Training Failed"),
  DEPLOYED("Deployed");
  private final String label;

  ModelStatus(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return label;
  }
}
