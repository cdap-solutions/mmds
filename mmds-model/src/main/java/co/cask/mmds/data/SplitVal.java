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

package co.cask.mmds.data;

import java.util.Objects;

/**
 * Value for test and training splits.
 *
 * @param <T> type of value
 */
public class SplitVal<T> {
  private final T train;
  private final T test;
  private final T total;

  public SplitVal(T train, T test, T total) {
    this.train = train;
    this.test = test;
    this.total = total;
  }

  public T getTrain() {
    return train;
  }

  public T getTest() {
    return test;
  }

  public T getTotal() {
    return total;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitVal<?> that = (SplitVal<?>) o;

    return Objects.equals(train, that.train) && Objects.equals(test, that.test) && Objects.equals(total, that.total);
  }

  @Override
  public int hashCode() {
    return Objects.hash(train, test, total);
  }
}
