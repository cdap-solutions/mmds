package co.cask.mmds.data;

/**
 * A count for test and training splits.
 */
public class SplitCountVal extends SplitVal<Long> {

  public SplitCountVal(long train, long test) {
    super(train, test, train + test);
  }
}
