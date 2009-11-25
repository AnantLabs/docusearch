package com.plexobject.docusearch.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.spatial.tier.projections.CartesianTierPlotter;
import org.apache.lucene.spatial.tier.projections.IProjector;
import org.apache.lucene.spatial.tier.projections.SinusoidalProjector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.converter.Constants;
import com.plexobject.docusearch.converter.Converters;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.Tuple;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.index.Indexer;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.lucene.analyzer.SimilarityHelper;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;

/**
 * @author Shahzad Bhatti
 * 
 */
// http://www.liferay.com/web/guest/community/forums/-/message_boards/message/4048205
// http://www.opensubscriber.com/message/java-user@lucene.apache.org/3646117.html
public class IndexerImpl implements Indexer {
    private static final Logger LOGGER = Logger.getLogger(IndexerImpl.class);
    private static final boolean OPTIMIZE = true;
    Pattern JSON_PATTERN = Pattern.compile("[,;:\\[\\]{}()\\s]+");
    private int numIndexed;
    private final Map<String, Boolean> INDEXED_FIELDS = new HashMap<String, Boolean>();
    private final ReentrantLock indexLock = new ReentrantLock();
    //
    private final String indexName;

    private final Directory dir;

    public IndexerImpl(final File dir) {
        this(LuceneUtils.toFSDirectory(dir), dir.getName());
    }

    public IndexerImpl(final Directory dir, final String indexName) {
        this.dir = dir;
        this.indexName = indexName;
    }

    @Override
    public int index(final IndexPolicy policy,
            final Iterator<List<Document>> docsIt, final boolean deleteExisting) {
        IndexWriter writer = null;
        IndexReader reader = null;
        IndexSearcher searcher = null;

        final Timer timer = Metric.newTimer("IndexerImpl.index");
        int succeeded = 0;
        try {
            indexLock.lock();
            Tuple tuple = open(policy);

            writer = tuple.first();
            reader = tuple.second();
            searcher = tuple.third();
            while (docsIt.hasNext()) {
                List<Document> docs = docsIt.next();
                for (Document doc : docs) {
                    try {
                        index(writer, policy, doc, searcher, deleteExisting);
                        succeeded++;

                        if (succeeded % 1000 == 0) {
                            timer.lapse("--succeeded indexing " + succeeded
                                    + " documents");
                        }
                    } catch (final Exception e) {
                        LOGGER.error("Error indexing " + doc, e);
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Error indexing documents", e);
        } finally {
            timer.stop("succeeded indexing " + succeeded
                    + " documents with support of dictionary?"
                    + policy.isAddToDictionary());
            close(writer, reader);
            try {
                if (policy.isAddToDictionary()) {
                    SimilarityHelper.getInstance().saveTrainingSpellChecker(
                            indexName);
                }
            } catch (Exception e) {
                LOGGER.error("failed to add spellings", e);
            }
            indexLock.unlock();
        }
        return succeeded;
    }

    IndexWriter createWriter(final IndexPolicy policy) throws IOException {
        return LuceneUtils.newWriter(dir, policy.getAnalyzer());
        // return LuceneUtils.newThreadedWriter(dir);

    }

    @SuppressWarnings("deprecation")
    private void index(final IndexWriter writer, final IndexPolicy policy,
            final Document doc, final IndexSearcher searcher,
            final boolean deleteExisting) throws CorruptIndexException,
            IOException {
        if (writer == null) {
            throw new NullPointerException("writer not specified");
        }
        if (policy == null) {
            throw new NullPointerException("index policy not specified");
        }
        if (doc == null) {
            throw new NullPointerException("document not specified");
        }
        if (searcher == null) {
            throw new NullPointerException("searcher not specified");
        }

        final Map<String, Object> map = doc.getAttributes();
        final JSONObject json = Converters.getInstance().getConverter(
                Object.class, JSONObject.class).convert(map);
        final org.apache.lucene.document.Document ldoc = new org.apache.lucene.document.Document();
        ldoc.add(new Field(Document.DATABASE, doc.getDatabase(),
                Field.Store.YES, Field.Index.NOT_ANALYZED));
        ldoc.add(new Field(Document.ID, doc.getId(), Field.Store.YES,
                Field.Index.NOT_ANALYZED));
        if (policy.hasOwner()) {
            ldoc.add(new Field(Constants.OWNER, policy.getOwner(),
                    Field.Store.YES, Field.Index.NOT_ANALYZED));
        }

        ldoc.add(new Field("indexDate", DateTools.dateToString(new Date(),
                DateTools.Resolution.DAY), Field.Store.YES,
                Field.Index.NOT_ANALYZED));

        if (policy.getBoost() > 0) {
            ldoc.setBoost(policy.getBoost());
        }
        if (policy.getScore() > 0) {
            ldoc.add(new Field("score", Integer.toString(policy.getScore()),
                    Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        }
        double spatialLatitude = 0;
        double spatialLongitude = 0;
        for (String name : doc.getAttributeNames()) {
            try {
                IndexPolicy.Field field = policy.getField(name);
                if (field == null) {
                    if (!INDEXED_FIELDS.containsKey(name)) {
                        INDEXED_FIELDS.put(name, Boolean.TRUE);
                    }
                    continue; // skip field that are not specified in the policy
                } else {
                    if (!INDEXED_FIELDS.containsKey(name)) {
                        INDEXED_FIELDS.put(name, Boolean.TRUE);
                    }
                }
                String value = getValue(json, name);
                if (value != null) {
                    if (value.length() > 0
                            && (field.sortableNumber || field.spatialLatitude || field.spatialLongitude)) {
                        double d = Double.valueOf(value);
                        if (field.spatialLatitude) {
                            spatialLatitude = d;
                        } else if (field.spatialLongitude) {
                            spatialLongitude = d;
                        }
                        value = NumericUtils.doubleToPrefixCoded(d);
                    }
                    final Field.Store store = field.storeInIndex ? Field.Store.YES
                            : Field.Store.NO;
                    final Field.Index index = field.tokenize ? Field.Index.TOKENIZED
                            : field.analyze ? Field.Index.ANALYZED
                                    : Field.Index.NOT_ANALYZED;
                    final Field.TermVector termVector = field.tokenize ? Field.TermVector.YES
                            : Field.TermVector.NO;
                    final Field locField = field.sortableNumber
                            || field.spatialLatitude || field.spatialLongitude ? new Field(
                            doc.getDatabase() + "." + name, value,
                            Field.Store.YES, Field.Index.NOT_ANALYZED)
                            : new Field(doc.getDatabase() + "." + name, value,
                                    store, index, termVector);
                    locField.setBoost(field.boost);
                    ldoc.add(locField);
                    if (policy.isAddToDictionary()) {
                        SimilarityHelper.getInstance().trainSpellChecker(
                                indexName, value);
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Indexing " + name + " using " + locField
                                + ", doc " + doc + ", policy " + policy);
                    }
                }
            } catch (JSONException e) {
                LOGGER.error("Failed to index value for " + name + " from "
                        + json + " due to ", e);
                throw new RuntimeException(e.toString());
            }
        }

        if (LOGGER.isInfoEnabled() && INDEXED_FIELDS.size() > 2) {
            LOGGER.info("Will index " + INDEXED_FIELDS.keySet() + " field for "
                    + indexName + " from " + doc.getDatabase());
        }
        //
        if (spatialLatitude != 0 && spatialLongitude != 0) {
            final IProjector projector = new SinusoidalProjector();
            int startTier = 5; // About 1000 mile bestFit
            final int endTier = 15; // about 1 mile bestFit
            for (; startTier <= endTier; startTier++) {
                CartesianTierPlotter ctp = new CartesianTierPlotter(startTier,
                        projector, Constants.TIER_PREFIX);
                final double boxId = ctp.getTierBoxId(spatialLatitude,
                        spatialLongitude);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("*********Adding field "
                            + ctp.getTierFieldName() + ":" + boxId
                            + ", spatialLatitude " + spatialLatitude
                            + ", spatialLongitude " + spatialLongitude);
                }
                ldoc.add(new Field(ctp.getTierFieldName(), NumericUtils
                        .doubleToPrefixCoded(boxId), Field.Store.YES,
                        Field.Index.NOT_ANALYZED_NO_NORMS));
            }
        }

        boolean newDocument = true;
        if (deleteExisting) {
            writer.deleteDocuments(LuceneUtils.docQuery(doc.getDatabase(), doc
                    .getId()));
        } else {
            final int[] count = new int[1];
            searcher.search(LuceneUtils
                    .docQuery(doc.getDatabase(), doc.getId()), null,
                    new Collector() {
                        @Override
                        public boolean acceptsDocsOutOfOrder() {
                            return false;
                        }

                        @Override
                        public void collect(int hits) throws IOException {
                            count[0] += hits;
                        }

                        @Override
                        public void setNextReader(IndexReader arg0, int arg1)
                                throws IOException {
                        }

                        @Override
                        public void setScorer(Scorer arg0) throws IOException {
                        }
                    });
            newDocument = count[0] == 0;
        }

        if (newDocument) {
            writer.addDocument(ldoc);
        } else {
            Term idTerm = new Term(Document.ID, doc.getId());
            writer.updateDocument(idTerm, ldoc);
        }

        if (++numIndexed % 1000 == 0 && LOGGER.isInfoEnabled()) {
            LOGGER.info(numIndexed + ": Indexing " + ldoc + " newDocument "
                    + newDocument + " for input " + doc + " with policy "
                    + policy);
        }
    }

    private String getValue(final JSONObject json, final String name)
            throws JSONException {
        String value = null;
        int ndx;
        if ((ndx = name.indexOf("[")) != -1) {
            value = getArrayValue(json, name, ndx);
        } else if ((ndx = name.indexOf("{")) != -1) {
            value = getHashValue(json, name, ndx);
        } else {
            value = json.optString(name);
        }
        if (value != null) {
            Matcher matcher = JSON_PATTERN.matcher(value);
            value = matcher.replaceAll(" ");
        }
        return value;
    }

    private String getHashValue(final JSONObject json, final String name,
            int ndx) throws JSONException {
        String value;
        final String tagName = name.substring(0, ndx);
        value = json.optString(tagName);
        if (value == null) {
            // do nothing
        } else if (!value.startsWith("{")) {
            throw new IllegalStateException("Failed to get hash value for "
                    + tagName + " in " + value + " from json " + json);
        } else {
            final JSONObject jsonObject = new JSONObject(value);
            final String subscript = name.substring(ndx + 1, name.indexOf("}"));
            value = jsonObject.optString(subscript);
        }
        return value;
    }

    private String getArrayValue(final JSONObject json, final String name,
            int ndx) throws JSONException {
        String value;
        final String subscript = name.substring(ndx + 1, name.indexOf("]"));

        final String tagName = name.substring(0, ndx);
        value = json.optString(tagName);
        if (value == null) {
            // do nothing
        } else if (!value.startsWith("[")) {
            if (value != null && value.startsWith("{")) {
                final JSONObject jsonObject = new JSONObject(value);
                value = jsonObject.optString(subscript);
            } else {
                LOGGER.warn("Failed to get array value for " + tagName + " in "
                        + value + " from json " + json);
            }
        } else {
            final JSONArray jsonArray = new JSONArray(value);
            try {
                int offset = Integer.parseInt(subscript);
                value = jsonArray.optString(offset);
            } catch (NumberFormatException e) {
                StringBuilder sb = new StringBuilder();
                int len = jsonArray.length();
                for (int i = 0; i < len; i++) {
                    JSONObject elementJson = jsonArray.getJSONObject(i);
                    sb.append(elementJson.getString(subscript));
                    sb.append(" ");
                }
                value = sb.toString();
                LOGGER.info("xxxxxxxxxxxxxxxxAdding entire array of " + name
                        + "=" + value);
            }
        }
        return value;
    }

    private Tuple open(final IndexPolicy policy) throws IOException {
        final IndexWriter writer = createWriter(policy);
        final IndexReader reader = IndexReader.open(dir, false);
        final IndexSearcher searcher = new IndexSearcher(reader);

        return new Tuple(writer, reader, searcher);
    }

    private void close(final IndexWriter writer, final IndexReader reader) {
        if (writer != null) {
            try {
                if (OPTIMIZE) {
                    writer.optimize();
                }
            } catch (Exception e) {
                LOGGER.error("failed to optimize", e);
            } finally {
                try {
                    writer.close();
                } catch (Exception e) {
                    LOGGER.error("failed to close", e);
                }
            }
        }
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                LOGGER.error("failed to close reader", e);
            }
        }
    }
}