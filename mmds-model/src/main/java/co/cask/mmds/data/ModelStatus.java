package co.cask.mmds.data;

import com.google.gson.annotations.SerializedName;

/**
 * Model status. The state transition diagram is:
 *
 *                                 |----------- assign split ------------------|
 *                                 |                                           |
 *                                 v       |-- split failed --> SPLIT_FAILED --|
 *   EMPTY -- assign split --> SPLITTING --|
 *                                 ^       |-- split succeeded --> DATA_READY -- assign modeler --> TRAINING
 *                                 |                                     |
 *                                 |-------------------------------------|
 *
 *      |--------------- assign modeler ------------------|
 *      |                                                 |-- assign split --> SPLITTING.
 *      v       |-- training failed --> TRAINING_FAILED --|
 *   TRAINING --|
 *              |-- training succeeded --> TRAINED -- deploy model --> DEPLOYED
 *
 *
 *
 * A model starts out in the EMPTY state. In the EMPTY state, it only has an id, name, and description.
 *
 * From the EMPTY state, it can be assigned a data split. If the split fails, the model moves to SPLIT_FAILED.
 * If the split succeeds, the model moves to DATA_READY.
 *
 * From the SPLIT_FAILED state, it can only be assigned a new data split, which moves it to SPLITTING.
 *
 * From the DATA_READY state, it can be assigned a new data split, which moves it to SPLITTING. It can also be
 * assigned a modeler (algorithm and hyperparameters), which moves it to TRAINING.
 *
 * From TRAINING, it moves to TRAINED if the model training succeeds.
 * If the training failed, it moves to TRAINING_FAILED.
 *
 * From the TRAINED state, it can be deployed, which moves it to DEPLOYED.
 *
 * From the TRAINING_FAILED state, it can be assigned a modeler, which moves it to the TRAINING state. It can also
 * be assigned a different split, which moves it back to the SPLITTING state.
 */
public enum ModelStatus {
  @SerializedName("Empty")
  EMPTY,
  @SerializedName("Splitting")
  SPLITTING,
  @SerializedName("Split Failed")
  SPLIT_FAILED,
  @SerializedName("Data Ready")
  DATA_READY,
  @SerializedName("Training")
  TRAINING,
  @SerializedName("Trained")
  TRAINED,
  @SerializedName("Training Failed")
  TRAINING_FAILED,
  @SerializedName("Deployed")
  DEPLOYED
}
