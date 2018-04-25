package co.cask.mmds.modeler.feature;

import com.google.common.collect.ImmutableList;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Generates features from raw data.
 */
public abstract class FeatureGenerator {
  protected final List<String> features;
  private final Set<String> categoricalFeatures;
  private PipelineModel featureGenModel;

  protected FeatureGenerator(List<String> features, Set<String> categoricalFeatures) {
    this.features = new ArrayList<>(features);
    this.categoricalFeatures = new HashSet<>(categoricalFeatures);
  }

  public List<String> getFeatures() {
    return Collections.unmodifiableList(features);
  }


  /**
   * Generates a new dataset that contains all features columns, a "_features" column that is a vector of all
   * double values of the features, and the target column.
   *
   * @param rawData the raw dataset
   * @param target the target column
   * @return dataset with added features column
   */
  public Dataset<Row> generateFeatures(Dataset<Row> rawData, String target) {
    return generateFeatures(rawData, ImmutableList.of(target));
  }

  /**
   * Generates a new dataset that has all features assembled into a vector in the "_features" column.
   *
   * @param rawData the raw dataset
   * @param extraColumns extra non-feature columns to keep around in the dataset
   * @return dataset with added features column
   */
  public Dataset<Row> generateFeatures(Dataset<Row> rawData, List<String> extraColumns) {
    // start out with x,y,z columns

    // add cleaned columns: x, y, z, _c_x, _c_y, _c_z
    // we add columns instead of modifying in place because the predictor needs to preserve original values
    // cleaned columns will have nulls replaced, with numeric columns using -1 and categorical using "?"
    int numFeatures = features.size();
    Column[] columnsWithCopies = new Column[2 * numFeatures + extraColumns.size()];
    int i = 0;
    // column -> null replacement value.
    Map<String, Object> nullValueMap = new HashMap<>();
    for (String originalFeature : features) {
      String cleanName = cleanName(originalFeature);
      columnsWithCopies[i] = new Column(originalFeature);
      Column featureCopy = new Column(originalFeature).as(cleanName);
      if (isCategorical(originalFeature)) {
        featureCopy = featureCopy.cast(DataTypes.StringType);
        nullValueMap.put(cleanName, "?");
      } else {
        featureCopy = featureCopy.cast(DataTypes.DoubleType);
        nullValueMap.put(cleanName, -1.d);
      }
      columnsWithCopies[numFeatures + i] = featureCopy;
      i++;
    }
    i = 2 * numFeatures;
    for (String extraColumn : extraColumns) {
      columnsWithCopies[i] = new Column(extraColumn);
      i++;
    }
    Dataset<Row> cleanData = rawData.select(columnsWithCopies).na().fill(nullValueMap);
    if (featureGenModel == null) {
      featureGenModel = getFeatureGenModel(cleanData);
    }

    return featureGenModel.transform(cleanData);
  }

  /**
   * @return the feature generation model. Returns null if features were not generated yet.
   */
  @Nullable
  public PipelineModel getFeatureGenModel() {
    return featureGenModel;
  }

  protected abstract PipelineModel getFeatureGenModel(Dataset<Row> cleanData);

  protected boolean isCategorical(String featureName) {
    return categoricalFeatures.contains(featureName);
  }

  protected String cleanName(String originalName) {
    return "_c_" + originalName;
  }

  protected String indexedName(String originalName) {
    return "_i_" + originalName;
  }

}
