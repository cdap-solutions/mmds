package co.cask.mmds.data;

import com.google.gson.annotations.SerializedName;

/**
 * Split states.
 */
public enum SplitStatus {
  @SerializedName("Splitting")
  SPLITTING,
  @SerializedName("Complete")
  COMPLETE,
  @SerializedName("Failed")
  FAILED
}
