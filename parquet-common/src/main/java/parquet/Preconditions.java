/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet;

/**
 * Utility for parameter validation
 *
 * @author Julien Le Dem
 *
 */
public final class Preconditions {
  private Preconditions() { }

  /**
   * @param o the param to check
   * @param name the name of the param for the error message
   * @return the validated o
   * @throws NullPointerException if o is null
   */
  public static <T> T checkNotNull(T o, String name) throws NullPointerException {
    if (o == null) {
      throw new NullPointerException(name + " should not be null");
    }
    return o;
  }

  /**
   * @param valid whether the argument is valid
   * @param message error message if the argument is not valid
   * @throws IllegalArgumentException if !valid
   */
  public static void checkArgument(boolean valid, String message) throws IllegalArgumentException {
    if (!valid) {
      throw new IllegalArgumentException(message);
    }
  }
}
