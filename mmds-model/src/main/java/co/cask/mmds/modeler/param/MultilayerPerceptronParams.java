package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.DoubleParam;
import co.cask.mmds.modeler.param.spec.IntParam;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.Range;
import co.cask.mmds.modeler.param.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Multilayer Perceptron.
 */
public class MultilayerPerceptronParams implements ModelerParams {
  private final IntParam blockSize;
  private final IntParam maxIterations;
  private final DoubleParam tolerance;
  private final DoubleParam stepSize;
  private final StringParam solver;

  public MultilayerPerceptronParams(Map<String, String> modelParams) {
    blockSize = new IntParam("blockSize",
                             "Block size for stacking input data in matrices to speed up the computation. " +
                               "Data is stacked within partitions. If block size is more than remaining data in " +
                               "a partition then it is adjusted to the size of this data. " +
                               "Recommended size is between 10 and 1000.",
                             128, new Range(1, true), modelParams);
    maxIterations = new IntParam("maxIterations", "maximum number of iterations",
                                 100, new Range(0, true), modelParams);
    tolerance = new DoubleParam("tolerance",
                                "Convergence tolerance of iterations. " +
                                  "Smaller values will lead to higher accuracy with the cost of more iterations.",
                                0.000001d, new Range(0d, true), modelParams);
    // only for 'gd' solver
    stepSize = new DoubleParam("stepSize",
                               "Step size to be used for each iteration of optimization. (only for 'gd' solver).",
                               0.03d, new Range(0d, false), modelParams);
    // "gd" (minibatch gradient descent) or "l-bfgs"
    solver = new StringParam("solver",
                             "The solver algorithm for optimization. " +
                               "'gd' uses minibatch gradient descent. " +
                               "'l-bfgs' uses Limited-memory BFGS, " +
                               "which is a limited-memory quasi-Newton optimization method.",
                             "l-bfgs", ImmutableSet.of("gd", "l-bfgs"), modelParams);
  }

  public void setParams(MultilayerPerceptronClassifier modeler) {
    modeler.setBlockSize(blockSize.getVal());
    modeler.setMaxIter(maxIterations.getVal());
    modeler.setTol(tolerance.getVal());
    modeler.setStepSize(stepSize.getVal());
    modeler.setSolver(solver.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(new HashMap<String, String>(), blockSize, maxIterations, tolerance, stepSize, solver);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(new ArrayList<ParamSpec>(), blockSize, maxIterations, tolerance, stepSize, solver);
  }
}
