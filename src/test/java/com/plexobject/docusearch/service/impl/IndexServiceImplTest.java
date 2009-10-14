package com.plexobject.docusearch.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.service.IndexService;
import com.plexobject.docusearch.service.impl.IndexServiceImpl;

public class IndexServiceImplTest {
	private static final String PEOPLE = "people";
	private static final String DB_NAME = "MYDB";
	private static final int MAX_LIMIT = 2048;
	private static Logger LOGGER = Logger.getRootLogger();

	private DocumentRepository repository;
	private ConfigurationRepository configRepository;
	private DocumentsDatabaseIndexer indexer;

	private IndexService service;

	@Before
	public void setUp() throws Exception {
		LOGGER.setLevel(Level.INFO);

		LOGGER.addAppender(new ConsoleAppender(new PatternLayout(
				PatternLayout.TTCC_CONVERSION_PATTERN)));
		repository = EasyMock.createMock(DocumentRepository.class);
		configRepository = EasyMock.createMock(ConfigurationRepository.class);
		indexer = new DocumentsDatabaseIndexer(new RepositoryFactory(
				repository, configRepository));
		service = new IndexServiceImpl(new RepositoryFactory(repository,
				configRepository), indexer);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testCreate() {
		EasyMock.expect(repository.getAllDocuments(PEOPLE, 0, MAX_LIMIT))
				.andReturn(newDocuments());

		EasyMock.expect(configRepository.getIndexPolicy(PEOPLE)).andReturn(
				newIndexPolicy());

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);

		Response response = service.create(PEOPLE);
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
		Assert.assertEquals(200, response.getStatus());
		Assert.assertTrue("unexpected response "
				+ response.getEntity().toString(), response.getEntity()
				.toString().contains("rebuilt index for " + PEOPLE));
	}

	@Test
	public final void testUpdate() {
		EasyMock.expect(repository.getDocument(PEOPLE, "id")).andReturn(
				newDocument(true));
		EasyMock.expect(configRepository.getIndexPolicy(PEOPLE)).andReturn(
				newIndexPolicy());

		EasyMock.replay(repository);
		EasyMock.replay(configRepository);

		Response response = service.update(PEOPLE, "id");
		EasyMock.verify(repository);
		EasyMock.verify(configRepository);
		Assert.assertEquals(200, response.getStatus());
		Assert.assertTrue("unexpected response "
				+ response.getEntity().toString(), response.getEntity()
				.toString().contains(
						"updated 1 documents in index for " + PEOPLE
								+ " with ids id"));
	}

	private List<Document> newDocuments() {
		return Arrays.asList(newDocument(true));

	}

	@SuppressWarnings("unchecked")
	private Document newDocument(final boolean specifyId) {
		final String id = specifyId ? "x" + System.nanoTime() : null;
		final Map<String, String> map = new TreeMap<String, String>();
		map.put("Y", "8");
		map.put("Z", "9");
		final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");
		final Document doc = new DocumentBuilder(DB_NAME).setId(id).put("A",
				"1").put("B", "2").put("C", map).put("D", arr).build();

		return doc;
	}

	private static IndexPolicy newIndexPolicy() {
		final IndexPolicy policy = new IndexPolicy();
		policy.setScore(10);
		policy.setBoost(20.5F);
		for (int i = 0; i < 10; i++) {
			policy.add("name" + i, i % 2 == 0, i % 2 == 1, i % 2 == 1, 1.1F);
		}
		return policy;
	}
}
