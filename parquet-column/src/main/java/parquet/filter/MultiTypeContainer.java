/**
 * Copyright 2014, Regents of the University of California
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
package parquet.filter;

import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.column.ColumnReader;
import parquet.io.api.Binary;

/**
 * Container class that can hold a fixed number of objects with varying types.
 * Accesses throw exceptions at runtime if they access with the incorrect type.
 *
 * @author Frank Austin Nothaft
 */
public final class MultiTypeContainer {

  // types of objects in container
  private final PrimitiveTypeName[] types;

  // values of objects
  private final Object[] values;

  /**
   * Private constructor. Use load instead.
   *
   * @see load
   */
  private MultiTypeContainer(PrimitiveTypeName[] t, Object[] v) {
    types = t;
    values = v;
  }

  public static MultiTypeContainer load(ColumnReader[] inputs,
                                        PrimitiveTypeName[] types) {
      
    if (inputs.length != types.length) {
      throw new IllegalArgumentException("Argument lengths are not the same.");
    }
      
    Object[] values = new Object[types.length];

    for (int j = 0; j < values.length; j++) {
      switch (types[j]) {
      case INT32:   Integer i = inputs[j].getInteger();
        values[j] = (Object) i;
        break;
      case INT64:   Long l = inputs[j].getLong();
        values[j] = (Object) l;
        break;
      case BOOLEAN: Boolean b = inputs[j].getBoolean();
        values[j] = (Object) b;
        break;
      case BINARY:  values[j] = (Object) inputs[j].getBinary();
        break;
      case FLOAT:   Float f = inputs[j].getFloat();
        values[j] = (Object) f;
        break;
      case DOUBLE:  Double d = inputs[j].getDouble();
        values[j] = (Object) d;
        break;
      }
    }

    return new MultiTypeContainer(types, values);
  }

  /**
   * Gets the int in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns an int, with type checked.
   */
  public int getInt(int i) {
    if (types[i] != PrimitiveTypeName.INT32)
      throw new IllegalArgumentException("Field " + i + " is not an int.");

    return (Integer) values[i];
  }
  
  /**
   * Gets the long in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns a long, with type checked.
   */
  public long getLong(int i) {
    if (types[i] != PrimitiveTypeName.INT64)
      throw new IllegalArgumentException("Field " + i + " is not a long.");

    return (Long) values[i];
  }
  
  /**
   * Gets the boolean in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns an int, with type checked.
   */
  public boolean getBoolean(int i) {
    if (types[i] != PrimitiveTypeName.BOOLEAN)
      throw new IllegalArgumentException("Field " + i + " is not a boolean.");

    return (Boolean) values[i];
  }
  
  /**
   * Gets the binary in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns a binary, with type checked.
   */
  public Binary getBinary(int i) {
    if (types[i] != PrimitiveTypeName.BINARY && 
        types[i] != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
      throw new IllegalArgumentException("Field " + i + " is not a binary.");

    return (Binary) values[i];
  }
  
  /**
   * Gets the string in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns a string, with type checked.
   */
  public String getString(int i) {
    if (types[i] != PrimitiveTypeName.BINARY)
      throw new IllegalArgumentException("Field " + i + " is not a string.");

    return ((Binary) values[i]).toStringUsingUTF8();
  }
  
  /**
   * Gets the float in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns a float, with type checked.
   */
  public float getFloat(int i) {
    if (types[i] != PrimitiveTypeName.FLOAT)
      throw new IllegalArgumentException("Field " + i + " is not a float.");

    return (Float) values[i];
  }
  
  /**
   * Gets the double in position _i_.
   *
   * @param i Location to fetch value from.
   * @return Returns a double, with type checked.
   */
  public double getDouble(int i) {
    if (types[i] != PrimitiveTypeName.DOUBLE)
      throw new IllegalArgumentException("Field " + i + " is not a double.");

    return (Double) values[i];
  }

}
