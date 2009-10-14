package com.plexobject.docusearch.lucene;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.index.lucene.ThreadedIndexWriter;

/**
 * @author bhatti@plexobject.com
 * 
 */
public final class LuceneUtils {
	private static final Logger LOGGER = Logger.getLogger(LuceneUtils.class);
	public static final Analyzer ANALYZER = new StandardAnalyzer(Version.LUCENE_CURRENT);

	public static final String DEFAULT_OPERATOR = System.getProperty(
			"lucene.operator", "OR");
	public static final File INDEX_DIR = new File(System
			.getProperty("user.home"), System.getProperty("lucene.dir",
			"lucene"));

	public static final int RAM_BUF = Integer.getInteger("lucene.ram", 16);

	public static final int BATCH_SIZE = Integer
			.getInteger("lucene.batch", 200);

	public static final int COMMIT_MIN = Integer.getInteger(
			"lucene.commit.min", 5000);

	private static final boolean LUCENE_DEBUG = Boolean
			.getBoolean("lucene.debug");

	private LuceneUtils() {
	}

	public static Query docQuery(final String viewname, final String id) {
		BooleanQuery q = new BooleanQuery();
		q.add(new TermQuery(new Term(Document.DATABASE, viewname)), Occur.MUST);
		q.add(new TermQuery(new Term(Document.ID, id)), Occur.MUST);
		return q;
	}

	public static IndexWriter newWriter(final Directory dir) throws IOException {
		final IndexWriter writer = new IndexWriter(dir, ANALYZER,
				MaxFieldLength.UNLIMITED);
		if (IndexWriter.isLocked(dir)) {
			LOGGER.warn("***Unlocking " + dir + " directory for IndexWriter");
			IndexWriter.unlock(dir);
		}
		return configWriter(writer);
	}

	public static IndexWriter newThreadedWriter(final Directory dir)
			throws IOException {
		final IndexWriter writer = new ThreadedIndexWriter(dir, ANALYZER, true,
				MaxFieldLength.UNLIMITED);
		return configWriter(writer);
	}

	public static String parseDate(String s) throws ParseException {
		return DateTools.dateToString(new SimpleDateFormat("yyyy-MM-dd")
				.parse(s), DateTools.Resolution.MILLISECOND);
	}

	private static IndexWriter configWriter(final IndexWriter writer) {
		// Customize merge policy.
		final LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy(writer);
		mp.setMergeFactor(Integer.MAX_VALUE);
		mp.setMaxMergeMB(1000);
		mp.setUseCompoundFile(false);
		writer.setMergePolicy(mp);

		writer.setRAMBufferSizeMB(RAM_BUF);

		if (LUCENE_DEBUG) {
			writer.setInfoStream(System.err);
		}

		return writer;
	}

	public static Directory toFSDirectory(final File dir) {
		// Create index directory if missing.
		if (!dir.exists()) {
			if (!dir.mkdirs()) {
				LOGGER.fatal("Unable to create index dir "
						+ dir.getAbsolutePath());
				throw new Error("Unable to create index dir "
						+ dir.getAbsolutePath());
			}
		}

		// Verify index directory is writable.
		if (!dir.canWrite()) {
			LOGGER.fatal(dir.getAbsolutePath() + " is not writable.");
			throw new Error(dir.getAbsolutePath() + " is not writable.");
		}

		try {
			final Directory d = FSDirectory.open(dir);

			// Check index prior to startup if it exists.
			if (IndexReader.indexExists(d)) {
				final CheckIndex check = new CheckIndex(d);
				final CheckIndex.Status status = check.checkIndex();
				if (status.clean) {
					LOGGER.debug("Index is clean.");
				} else {
					LOGGER.warn("Index is not clean.");
				}
			}
			return d;

		} catch (IOException e) {
			LOGGER.error("Failed to unlock index", e);
			throw new RuntimeException(e);
		}
	}

}
