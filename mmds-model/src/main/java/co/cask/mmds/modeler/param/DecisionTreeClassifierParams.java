package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.tree.DecisionTreeParams;
import org.apache.spark.ml.tree.DecisionTreeRegressorParams;

import java.util.List;
import java.util.Map;

/**
 * Common modeler parameters for tree based algorithms.
 */
public class DecisionTreeClassifierParams extends TreeParams {
  private StringParam impurity;

  public DecisionTreeClassifierParams(Map<String, String> modelParams) {
    super(modelParams);
    this.impurity = new StringParam("impurity", "Criterion used for information gain calculation", "gini",
                                    ImmutableSet.of("gini", "entropy"), modelParams);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), impurity);
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), impurity);
  }

  public void setParams(DecisionTreeRegressorParams params) {
    setParams((DecisionTreeParams) params);
    params.setImpurity(impurity.getVal());
  }
}
