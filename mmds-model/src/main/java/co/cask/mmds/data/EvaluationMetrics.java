package co.cask.mmds.data;


import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Evaluation metrics for a model.
 */
public class EvaluationMetrics {
  private Double precision;
  private Double recall;
  private Double f1;
  private Double rmse;
  private Double r2;
  private Double evariance;
  private Double mae;

  public EvaluationMetrics(double precision, double recall, double f1) {
    this(precision, recall, f1, null, null, null, null);
  }

  public EvaluationMetrics(double rmse, double r2, double evariance, double mae) {
    this(null, null, null, rmse, r2, evariance, mae);
  }

  public EvaluationMetrics(@Nullable Double precision, @Nullable Double recall, @Nullable Double f1,
                           @Nullable Double rmse, @Nullable Double r2, @Nullable Double evariance,
                           @Nullable Double mae) {
    this.precision = precision;
    this.recall = recall;
    this.f1 = f1;
    this.rmse = rmse;
    this.r2 = r2;
    this.evariance = evariance;
    this.mae = mae;
  }

  @Nullable
  public Double getPrecision() {
    return precision;
  }

  @Nullable
  public Double getRecall() {
    return recall;
  }

  @Nullable
  public Double getF1() {
    return f1;
  }

  @Nullable
  public Double getRmse() {
    return rmse;
  }

  @Nullable
  public Double getR2() {
    return r2;
  }

  @Nullable
  public Double getEvariance() {
    return evariance;
  }

  @Nullable
  public Double getMae() {
    return mae;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EvaluationMetrics that = (EvaluationMetrics) o;

    return Objects.equals(precision, that.precision) &&
      Objects.equals(recall, that.recall) &&
      Objects.equals(f1, that.f1) &&
      Objects.equals(rmse, that.rmse) &&
      Objects.equals(r2, that.r2) &&
      Objects.equals(evariance, that.evariance) &&
      Objects.equals(mae, that.mae);
  }

  @Override
  public int hashCode() {
    return Objects.hash(precision, recall, f1, rmse, r2, evariance, mae);
  }
}