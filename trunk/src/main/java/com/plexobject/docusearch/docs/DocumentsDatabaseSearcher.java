package com.plexobject.docusearch.docs;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import org.apache.lucene.store.Directory;

import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.lucene.LuceneUtils;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.query.Query;
import com.plexobject.docusearch.query.QueryCriteria;
import com.plexobject.docusearch.query.QueryPolicy;
import com.plexobject.docusearch.query.SearchDoc;
import com.plexobject.docusearch.query.SearchDocList;
import com.plexobject.docusearch.query.lucene.QueryImpl;

public class DocumentsDatabaseSearcher {

	private static final Logger LOGGER = Logger
			.getLogger(DocumentsDatabaseSearcher.class);
	private static final int MAX_LIMIT = 2048;

	private final DocumentRepository repository;
	private final ConfigurationRepository configRepository;

	public DocumentsDatabaseSearcher(final RepositoryFactory repositoryFactory) {
		this.repository = repositoryFactory.getDocumentRepository();
		this.configRepository = repositoryFactory.getConfigurationRepository();
	}

	public Collection<Document> query(final String database,
			final String keywords, final int startkey, final int limit) {
		QueryPolicy policy = configRepository.getQueryPolicy(database);
		return query(database, keywords, startkey, limit, policy.getFields());
	}

	public Collection<Document> query(final String database,
			final String keywords, final int startkey, final int limit,
			final Collection<String> fields) {
		final File dir = new File(LuceneUtils.INDEX_DIR, database);
		return query(database, LuceneUtils.toFSDirectory(dir), keywords, startkey, limit, fields);
	}

	public Collection<Document> query(final String database, final Directory dir,
			final String keywords, final int startkey, final int limit,
			final Collection<String> fields) {
		final QueryCriteria criteria = new QueryCriteria()
				.setKeywords(keywords).addFields(fields);
		final Query query = new QueryImpl(dir);
		SearchDocList results = query.search(criteria, startkey, limit);
		Collection<Document> docs = new ArrayList<Document>();
		for (SearchDoc result : results) {
			Document doc = repository.getDocument(database, result.getId());
			docs.add(doc);
		}
		return docs;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Logger root = Logger.getRootLogger();
		root.setLevel(Level.INFO);

		root.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));

		final String database = args.length > 0 ? args[0] : "myindex";
		final String keywords = args.length > 1 ? args[1] : "Pope";

		int startkey = 0;
		int i = 0;
		final long started = System.currentTimeMillis();
		Collection<Document> docs = null;
		final DocumentsDatabaseSearcher searcher = new DocumentsDatabaseSearcher(
				new RepositoryFactory());

		while ((docs = searcher.query(database, keywords, startkey, MAX_LIMIT))
				.size() > 0) {
			for (Document doc : docs) {
				LOGGER.info(i + "th " + doc);
				i++;
			}
			startkey += docs.size();
		}
		final long elapsed = System.currentTimeMillis() - started;
		LOGGER.info("searched " + startkey + " records of " + database + " in "
				+ elapsed + " milliseconds.");
	}
}
