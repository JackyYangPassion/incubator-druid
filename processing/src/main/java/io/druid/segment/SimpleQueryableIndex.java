/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 */
public class SimpleQueryableIndex implements QueryableIndex
{
  private final Interval dataInterval;
  private final Indexed<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;

  private final Map<String, Supplier<Column>> columns;
  private final SmooshedFileMapper fileMapper;
  @Nullable
  private final Metadata metadata;
  private final Supplier<Map<String, DimensionHandler>> dimensionHandlers;

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> columnNames,

      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<Column>> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      boolean lazy
  )
  {
    Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
    this.dataInterval = Preconditions.checkNotNull(dataInterval, "dataInterval");
    this.columnNames = columnNames;
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;

    if (lazy) {
      this.dimensionHandlers = Suppliers.memoize(() -> {
            Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
            for (String dim : availableDimensions) {
              ColumnCapabilities capabilities = getColumn(dim).getCapabilities();
              DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
              dimensionHandlerMap.put(dim, handler);
            }
            return dimensionHandlerMap;
          }
      );
    } else {
      Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
      for (String dim : availableDimensions) {
        ColumnCapabilities capabilities = getColumn(dim).getCapabilities();
        DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
        dimensionHandlerMap.put(dim, handler);
      }
      this.dimensionHandlers = () -> dimensionHandlerMap;
    }
  }

  @VisibleForTesting
  public SimpleQueryableIndex(
      Interval interval,
      Indexed<String> columnNames,
      Indexed<String> availableDimensions,
      BitmapFactory bitmapFactory,

      Map<String, Supplier<Column>> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      Supplier<Map<String, DimensionHandler>> dimensionHandlers
  )
  {
    this.dataInterval = interval;
    this.columnNames = columnNames;
    this.availableDimensions = availableDimensions;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
    this.dimensionHandlers = dimensionHandlers;
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return columns.get(Column.TIME_COLUMN_NAME).get().getLength();
  }

  @Override
  public Indexed<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    return bitmapFactory;
  }

  @Nullable
  @Override
  public Column getColumn(String columnName)
  {
    Supplier<Column> columnHolderSupplier = columns.get(columnName);
    return columnHolderSupplier == null ? null : columnHolderSupplier.get();
  }

  @VisibleForTesting

  public Map<String, Supplier<Column>> getColumns()
  {
    return columns;
  }

  @VisibleForTesting
  public SmooshedFileMapper getFileMapper()
  {
    return fileMapper;
  }

  @Override
  public void close() throws IOException
  {
    fileMapper.close();
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return dimensionHandlers.get();
  }

}
