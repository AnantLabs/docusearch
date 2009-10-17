package com.plexobject.docusearch.lucene.analyzer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.analysis.SynonymFilter;
import org.apache.solr.analysis.SynonymMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.lucene.LuceneTestUtils;

public class SynonymAnalyzerTest {
	private static final String INPUT = "The quick brown fox jumps over the lazy dogs";
	private IndexSearcher searcher;
	private SynonymMap synonymMap;
	private Analyzer analyzer;
	private static Logger LOGGER = Logger.getRootLogger();

	@Before
	public void setUp() throws Exception {
		LOGGER.setLevel(Level.INFO);

		LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));
		synonymMap = new SynonymMap(true);

		synonymMap.add(Arrays.asList("jumps"), SynonymMap.makeTokens(Arrays
				.asList("jumps", "hops", "leaps")), false, true);
		synonymMap.add(Arrays.asList("hops"), SynonymMap.makeTokens(Arrays
				.asList("jumps", "hops", "leaps")), false, true);
		synonymMap.add(Arrays.asList("leaps"), SynonymMap.makeTokens(Arrays
				.asList("jumps", "hops", "leaps")), false, true);

		analyzer = new SynonymAnalyzer(synonymMap);

		RAMDirectory directory = new RAMDirectory();

		IndexWriter writer = new IndexWriter(directory, analyzer,
				IndexWriter.MaxFieldLength.UNLIMITED);
		Document doc = new Document();
		doc.add(new Field("content", INPUT, Field.Store.YES,
				Field.Index.ANALYZED));
		writer.addDocument(doc);

		writer.close();

		searcher = new IndexSearcher(directory, false);

	}

	@After
	public void tearDown() throws Exception {
		searcher.close();
	}

	@Test
	public void testTokens() throws Exception {

		String[] keywords = new String[] { "hops", "leaps" };
		for (String keyword : keywords) {
			List<Token> toks = getTokList(keyword, true);
			Assert.assertEquals("unexpected tokens " + toks, 3, toks.size());
			Assert.assertEquals("jumps", toks.get(0).term());
			Assert.assertEquals("hops", toks.get(1).term());
			Assert.assertEquals("leaps", toks.get(2).term());
		}
	}

	@Test
	public void testSearchByAPI() throws Exception {

		String[] keywords = new String[] { "jumps", "hops", "leaps" };
		for (String keyword : keywords) {
			TermQuery tq = new TermQuery(new Term("content", keyword));
			Assert.assertEquals("unexpected result for " + keyword + " using "
					+ synonymMap, 1, LuceneTestUtils.hitCount(searcher, tq));

			PhraseQuery pq = new PhraseQuery();
			pq.add(new Term("content", keyword));
			Assert.assertEquals("unexpected result for " + keyword, 1,
					LuceneTestUtils.hitCount(searcher, pq));
		}
	}

	SynonymFilter getFilter(String input) {
		final List<Token> toks = LuceneTestUtils.tokens(input);
		TokenStream ts = new TokenStream() {
			Iterator<Token> iter = toks.iterator();

			@Override
			public Token next() throws IOException {
				return iter.hasNext() ? (Token) iter.next() : null;
			}
		};

		return new SynonymFilter(ts, synonymMap);
	}

	List<Token> getTokList(String input, boolean includeOrig)
			throws IOException {
		List<Token> lst = new ArrayList<Token>();

		SynonymFilter sf = getFilter(input);

		Token target = new Token();
		while (true) {
			Token t = sf.next(target);
			if (t == null)
				break;
			lst.add((Token) t.clone());
		}
		return lst;
	}

}