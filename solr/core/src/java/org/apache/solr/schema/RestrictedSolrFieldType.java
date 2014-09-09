package org.apache.solr.schema;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.List;
import java.util.regex.PatternSyntaxException;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.lucure.core.RestrictedField;
import com.lucure.core.query.AuthQuery;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class RestrictedSolrFieldType extends AbstractSubTypeFieldType {

  private static final Logger LOG = LoggerFactory.getLogger(RestrictedSolrFieldType.class);

  public static final ColumnVisibility EMPTY_VISIBILITY =
      new ColumnVisibility();
  public static final char VISIBILITY_LOWER_BOUND = ']';
  public static final char VISIBILITY_UPPER_BOUND = '[';

  @Override
  public List<IndexableField> createFields(
      final SchemaField field, Object value, float boost) {

    String val_str = value.toString();
    ColumnVisibility cv = EMPTY_VISIBILITY;
    if(val_str != null && val_str.length() > 0) {
      //Column Visibility will be at the end of the string with the form:
      //  somestring[ColumnVisibility]
      if(val_str.charAt(val_str.length() - 1) == VISIBILITY_LOWER_BOUND) {
        int visibilityLoc = val_str.lastIndexOf(VISIBILITY_UPPER_BOUND);
        if(visibilityLoc != -1) {
          //includes column visibility
          val_str = val_str.substring(0, visibilityLoc);
          String cv_str = val_str.substring(visibilityLoc + 1, val_str.length() - 1);
          try {
            cv = new ColumnVisibility(cv_str);
          } catch(PatternSyntaxException pe) {
            LOG.warn("Column Visibility String["+cv_str+"] is not correctly formed", pe);
          }
        }
      }
    }

    LOG.debug("Saving field[{}] with value[{}], boost[{}], and ColumnVisibility[{}]",
        field, val_str, boost, cv);

    List<IndexableField> fields = subType.createFields(field, val_str,
        boost);
    final ColumnVisibility finalCv = cv;
    return FluentIterable.from(fields).transform(new Function<IndexableField, IndexableField>() {
      @Override
      public IndexableField apply(IndexableField input) {
        if (field.hasDocValues() && input.fieldType().docValueType() == null) {
          // restricted fields cannot handle docvalues at the moment
          throw new UnsupportedOperationException("This field type does not support doc values: " + this);
        }
        Object value = input.stringValue();
        if (value == null) {
          value = input.binaryValue();
          if (value == null) {
            value = input.numericValue();
            if (value == null) {
              value = input.readerValue();
            }
          }
        }

        if (value == null) {
          return null;
        }

        //TODO: Fix cast to FieldType
        FieldType fieldType = (FieldType) input.fieldType();

        //indexOptions has to be at least Freqs and Positions
        FieldInfo.IndexOptions indexOptions = fieldType.indexOptions();
        switch (indexOptions) {
          case DOCS_ONLY:
          case DOCS_AND_FREQS:
            indexOptions =
                FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            break;
          default:
            break;
        }

        fieldType.setIndexOptions(indexOptions);
        return new RestrictedField(input.name(), value, fieldType,
            finalCv);
      }
    }).filter(Predicates.notNull()).toList();
  }

  @Override
  public void write(
      TextResponseWriter writer, String name, IndexableField f)
      throws IOException {
    //TODO: Write out Column Visibility somehow too
    subType.write(writer, name, f);
    if(f instanceof RestrictedField) {
      writer.writeStr("cv", ((RestrictedField)f).getColumnVisibility().toString(), false);
    }
  }

  @Override
  public SortField getSortField(
      SchemaField field, boolean top) {
    return subType.getSortField(field, top);
  }

  @Override
  public Query getFieldQuery(
      QParser parser, SchemaField field, String externalVal) {
    String auth = null;
    SolrParams localParams = parser.getLocalParams();
    if (localParams != null) {
      auth = localParams.get("auth");
    }

    SolrParams params = parser.getParams();
    if (params != null) {
      auth = params.get("auth");
    }
    Query fieldQuery = subType.getFieldQuery(parser, field, externalVal);
    return new AuthQuery(fieldQuery, auth != null ? new Authorizations(auth.split(",")) : Authorizations.EMPTY);
  }

  @Override
  public Analyzer getIndexAnalyzer() {
    return subType.getIndexAnalyzer();
  }

  @Override
  public Analyzer getQueryAnalyzer() {
    return subType.getQueryAnalyzer();
  }

  @Override
  public Analyzer getAnalyzer() {
    return subType.getAnalyzer();
  }

  @Override
  public Similarity getSimilarity() {
    return subType.getSimilarity();
  }

  @Override
  public SimilarityFactory getSimilarityFactory() {
    return subType.getSimilarityFactory();
  }

  @Override
  public FieldType.NumericType getNumericType() {
    return subType.getNumericType();
  }

  @Override
  public void setSimilarity(
      SimilarityFactory similarityFactory) {
    subType.setSimilarity(similarityFactory);
  }

  @Override
  public String getPostingsFormat() {
    return subType.getPostingsFormat();
  }

}
