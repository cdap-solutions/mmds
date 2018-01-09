package co.cask.mmds.data;

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
  EMPTY("Empty"),
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
