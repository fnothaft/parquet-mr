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

import parquet.column.ColumnReader;
import static parquet.Preconditions.checkNotNull;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import java.util.Arrays;

/**
 * Record filter which applies the supplied predicate to multiple specified columns.
 *
 * @author Frank Austin Nothaft
 */
public final class MultiColumnRecordFilter implements RecordFilter {

  private final ColumnReader[] filterOnColumns;
  private final ColumnPredicates.MultiColumnPredicate filterPredicate;

  /**
   * Factory method for record filter which applies the supplied predicate to the specified column.
   * Note that if searching for a repeated sub-attribute it will only ever match against the
   * first instance of it in the object.
   *
   * @param columnPath Dot separated path specifier, e.g. "engine.capacity"
   * @param predicate Should call getBinary etc. and check the value
   */
  public static final UnboundRecordFilter columns(final String[] columnPaths,
                                                  final ColumnPredicates.MultiColumnPredicate predicate) {
    checkNotNull(predicate,  "predicate");
    return new UnboundRecordFilter() {
      @Override
      public RecordFilter bind(Iterable<ColumnReader> readers) {
        ColumnReader[] filterColumns = new ColumnReader[columnPaths.length];

        // loop over provided columns
        for (int i = 0; i < columnPaths.length; i++) {
          checkNotNull(columnPaths[i], "columnPath");
          
          boolean found = false;
          
          final String[] filterPath = columnPaths[i].split("\\.");
          for (ColumnReader reader : readers) {
            if ( Arrays.equals( reader.getDescriptor().getPath(), filterPath)) {
              filterColumns[i] = reader;
              found = true;
              break;
            }
          }
          
          // if we haven't found this path, throw an exception
          if (!found)
            throw new IllegalArgumentException( "Column " + columnPaths[i] + " does not exist.");
        }

        return new MultiColumnRecordFilter(filterColumns, predicate);
      }
    };
  }

  /**
   * Private constructor. Use columns() instead.
   */
  private MultiColumnRecordFilter(ColumnReader[] filterOnColumns, 
                                  ColumnPredicates.MultiColumnPredicate filterPredicate) {
    this.filterOnColumns = filterOnColumns;
    this.filterPredicate = filterPredicate;
  }

  /**
   * @return true if the current value for the column reader matches the predicate.
   */
  @Override
  public boolean isMatch() {
    return filterPredicate.apply(filterOnColumns);
  }

}
