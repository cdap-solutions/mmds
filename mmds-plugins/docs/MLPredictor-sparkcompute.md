# Machine Learning Predictor

Description
-----------

Uses a model trained by the ModelTrainer plugin to add a prediction field to incoming records.
The same features used to train the model must be present in each input record, but input records can also
contain additional non-feature fields. If the trained model uses categorical features,
and if the record being predicted contains new categories, that record will be dropped.
For example, suppose categorical feature 'city' was used to train a model that predicts housing prices.
If an incoming record has 'New York' as the city, but 'New York' was not in the training set,
that record will be dropped.


Properties
----------

**experimentId:** The ID of the experiment the model belongs to.
Experiment IDs can only contain alphanumeric characters, '.', '_', or '-'.

**modelId:** The ID of the model to use for predictions. Model IDs are unique within an experiment.
Model IDs can only contain alphanumeric characters, '.', '_', or '-'.

**predictionField:** The field in the output schema to place the prediction. Must be a double for regression models.
For classifier models, the prediction field can be a double or a string.
During the process of classifier model training, outcome fields will be assigned a unique double.
For example, the value 'sports' might be assigned value 0.0, and the value 'news' might be assigned value 1.0.
If you would like the prediction to use the original string value, make it of type string.
Otherwise, it should be of type double.

**schema:** The output schema, which must include the prediction field.
Must only contain fields from the input schema and the new prediction field.

**modelDataset:** The name of the fileset that stores trained models. Defaults to 'models'.
This should be set to the same value used by the ModelTrainer that trained the model.

**modelMetaDataset:** The name of the table that stores model metadata. Defaults to 'modelmeta'.
This should be set to the same value used by the ModelTrainer that trained the model.
