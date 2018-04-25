# Model Trainer

Description
-----------

Uses a Machine Learning algorithm to train a model. Input data must contain a outcome field and one or more feature
fields.


Properties
----------

**experimentId:** The ID of the experiment the model belongs to.
Experiment IDs can only contain alphanumeric characters, '.', '_', or '-'.

**modelId:** The ID of the model to use for predictions. Model IDs are unique within an experiment.
Model IDs can only contain alphanumeric characters, '.', '_', or '-'.

**modelDescription:** Description for the model.

**outcomeField:** The outcome field. If the field is a string or a boolean, it will be treated as a category.
If a record has a null outcome field, it will be dropped and will not be used for training the model.

**outcomeIsCategorical:** Whether the outcome is categorical. If the outcome field is a string or boolean,
it is always treated as categorical, regardless of what this value is. This is only used if the outcome field
is not a string or boolean. Defaults to false.

**algorithm:** The modeling algorithm to use to train the model.

**trainingParameters:** Map of training parameters. Different modeling algorithms use different parameters.

**predictionsDataset:** The name of the fileset that stores the predictions computed for the test data.
If not specified, the predictions will not be stored. The predictions dataset is partitioned by the experiment
and model ids, but it has a single schema for exploration through sql queries.
This means that all models that use the same predictions dataset must use the same features of the same getType,
or exploration will not work correctly. Predictions will be written out in csv format.

**predictionsDatasetDelimiter:** The delimiter used to separate fields when writing to the predictions dataset.
Defaults to a comma.

**featureWhitelist:** The features to include in the model. Must not be specified if a feature blacklist is specified.
Every field in the whitelist must be present in the input schema, or deployment will fail.
Feature fields must be of type string, boolean, int, long, float, or double.
Fields that are of type string or boolean will be treated as categorical features.
Fields that are of a numeric type will be treated as numeric features unless they are included in
the categoricalFeatures property.
If neither a feature blacklist nor whitelist is specified, every non-outcome field will be used as a feature.

**featureBlacklist:** The features to exclude from the model. Must not be specified if a feature whitelist is specified.
Fields that are of type string or boolean will be treated as categorical features.
Fields that are of a numeric type will be treated as continuous features unless they are included in
the categoricalFeatures property.
If neither a feature blacklist nor whitelist is specified, every non-outcome field will be used as a feature.

**categoricalFeatures:** Features that are categorical. Fields of type string and boolean are automatically treated as
categorical features. This only needs to be specified is there are numeric fields that should be treated
as categorical features.

**testSplitPercentage:** What percentage of the input data should be used as test data, specified as an integer
Defaults to 10.

**overwrite:** Whether to overwrite an existing model if it already exists.
If false, an error is thrown if there is an existing model. Defaults to false.

**modelDataset:** The name of the fileset that stores trained models. Defaults to 'models'.
This should be set to the same value used by the ModelTrainer that trained the model.

**modelMetaDataset:** The name of the table that stores model metadata. Defaults to 'modelmeta'.
This should be set to the same value used by the ModelTrainer that trained the model.

Algorithms
----------

Algorithms are divided into two types: Regression and Classification.
Regression algorithms predict a numeric value like a house price.
Classification algorithms predict a category, like whether an employee is likely to leave a company.

Regression Algorithms
---------------------

linear.regression

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxIterations         | int     | Maximum number of iterations                                           |
 | standardization       | boolean | Whether to standardize the training features before fitting the model  |
 | regularizationParam   | double  | Regularization param for L2 regularization                             |
 | elasticNetParam       | double  | ElasticNet mixing parameter. 0 is L2, 1 is L1, in between is a mixture |
 | tolerance             | double  | Convergence tolerance of iterations                                    |
 | fitIntercept          | boolean | If the intercept should be fit                                         |
 | solver                | string  | solver algorithm for optimization. l-bfgs, normal, or auto             |


generalized.linear.regression

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxIterations         | int     | Maximum number of iterations                                           |
 | regularizationParam   | double  | Regularization param for L2 regularization                             |
 | tolerance             | double  | Convergence tolerance of iterations                                    |
 | fitIntercept          | boolean | If the intercept should be fit                                         |
 | family                | string  |                                                                        |
 | link                  | string  |                                                                        |


decision.tree.regression

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxDepth              | int     | Maximum tree depth                                                     |
 | maxBins               | int     | Maximum number of bins                                                 |
 | minInstancesPerNode   | int     | Minimum number of data points per tree node                            |
 | minInfoGain           | double  | Stop training when info gain between iterations is less than this      |
 | impurity              | string  | Criterion for info gain calculation. entropy or gini                   |
 | seed                  | long    | Seed for randomization                                                 |


random.forest.regression

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxDepth              | int     | Maximum tree depth                                                     |
 | maxBins               | int     | Maximum number of bins                                                 |
 | minInstancesPerNode   | int     | Minimum number of data points per tree node                            |
 | minInfoGain           | double  | Stop training when info gain between iterations is less than this      |
 | impurity              | string  | Criterion for info gain calculation. entropy or gini                   |
 | seed                  | long    | Seed for randomization                                                 |
 | numTrees              | int     | Number of trees to train (>= 1). If 1, no bootstrapping is used        |
 | subsamplingRate       | double  | Faction of training data used for each decision tree, in range (0, 1]  |
 | featureSubsetStrategy | string  | auto, all, onethird, sqrt, log2, or n (0, 1.0]                         |


gradient.boosted.tree.regression

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxDepth              | int     | Maximum tree depth                                                     |
 | maxBins               | int     | Maximum number of bins                                                 |
 | minInstancesPerNode   | int     | Minimum number of data points per tree node                            |
 | minInfoGain           | double  | Stop training when info gain between iterations is less than this      |
 | impurity              | string  | Criterion for info gain calculation. entropy or gini                   |
 | seed                  | long    | Seed for randomization                                                 |
 | maxIterations         | int     | Maximum number of iterations                                           |
 | lossType              | double  | Loss function to minimize. L2 or L1                                    |
 | subsamplingRate       | double  | Faction of training data used for each decision tree, in range (0, 1]  |
 | stepSize              | string  | Learning rate for shrinking estimator contributions, in range (0, 1]   |


Classification Algorithms
-------------------------

logistic.regression

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxIterations         | int     | Maximum number of iterations                                           |
 | standardization       | boolean | Whether to standardize the training features before fitting the model  |
 | regularizationParam   | double  | Regularization param for L2 regularization                             |
 | elasticNetParam       | double  | ElasticNet mixing parameter. 0 is L2, 1 is L1, in between is a mixture |
 | tolerance             | double  | Convergence tolerance of iterations                                    |
 | threshold             | double  | Threshold in binary classification prediction, in range [0, 1]         |


naive.bayes

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | smoothing             | double  | Smoothing parameter                                                    |
 | type                  | string  | model type, multinomial or bernoulli                                   |


decision.tree.classifier

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxDepth              | int     | Maximum tree depth                                                     |
 | maxBins               | int     | Maximum number of bins                                                 |
 | minInstancesPerNode   | int     | Minimum number of data points per tree node                            |
 | minInfoGain           | double  | Stop training when info gain between iterations is less than this      |
 | impurity              | string  | Criterion for info gain calculation. entropy or gini                   |
 | seed                  | long    | Seed for randomization                                                 |


random.forest.classifier

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxDepth              | int     | Maximum tree depth                                                     |
 | maxBins               | int     | Maximum number of bins                                                 |
 | minInstancesPerNode   | int     | Minimum number of data points per tree node                            |
 | minInfoGain           | double  | Stop training when info gain between iterations is less than this      |
 | impurity              | string  | Criterion for info gain calculation. entropy or gini                   |
 | seed                  | long    | Seed for randomization                                                 |
 | numTrees              | int     | Number of trees to train (>= 1). If 1, no bootstrapping is used        |
 | subsamplingRate       | double  | Faction of training data used for each decision tree, in range (0, 1]  |
 | featureSubsetStrategy | string  | auto, all, onethird, sqrt, log2, or n (0, 1.0]                         |


gradient.boosted.tree.classifier

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | maxDepth              | int     | Maximum tree depth                                                     |
 | maxBins               | int     | Maximum number of bins                                                 |
 | minInstancesPerNode   | int     | Minimum number of data points per tree node                            |
 | minInfoGain           | double  | Stop training when info gain between iterations is less than this      |
 | impurity              | string  | Criterion for info gain calculation. entropy or gini                   |
 | seed                  | long    | Seed for randomization                                                 |
 | maxIterations         | int     | Maximum number of iterations                                           |
 | lossType              | double  | Loss function to minimize. L2 or L1                                    |
 | subsamplingRate       | double  | Faction of training data used for each decision tree, in range (0, 1]  |
 | stepSize              | string  | Learning rate for shrinking estimator contributions, in range (0, 1]   |


multilayer.perceptron.classifier

 | parameter name        | type    | description                                                            |
 |-----------------------|---------|------------------------------------------------------------------------|
 | blockSize             | int     | Block size for stacking input matrix data for speedup, in [10, 1000]   |
 | maxIterations         | int     | Maximum number of iterations                                           |
 | tolerance             | double  | Convergence tolerance of iterations                                    |
 | stepSize              | double  | step size for gd (gradient descent) solver                             |
 | seed                  | long    | Seed for randomization                                                 |
 | solver                | string  | Solver algorithm for optimization. gd or l-bfgs                        |
