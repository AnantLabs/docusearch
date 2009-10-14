package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;

public class BiRelationMergerTest {
	private static final String DB_NAME = "MYDB";

	private DocumentRepository repository;
	private ConfigurationRepository configRepository;

	@Before
	public void setUp() throws Exception {
		repository = EasyMock.createMock(DocumentRepository.class);
		configRepository = EasyMock.createMock(ConfigurationRepository.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testMergeListOfDocument() {
	}

	@Test
	public final void testMergeDocument() {
	}

	@Test(expected = NullPointerException.class)
	public final void testCreateMergerWithNullProperties() {
		new BiRelationMerger(repository, (Properties) null);

	}

	@Test(expected = NullPointerException.class)
	public final void testCreateMergerWithNullFile() throws IOException {
		new BiRelationMerger(repository, (File) null);

	}

	@Test(expected = IllegalArgumentException.class)
	public final void testCreateMergerWithoutProperties() {
		Properties props = new Properties();
		BiRelationMerger merger = new BiRelationMerger(repository, props);
		Assert.assertNotNull(merger);
	}

	@Test
	public final void testRun() {
		EasyMock.expect(repository.createDatabase(DB_NAME)).andReturn(true);
		Properties props = new Properties();
		//BiRelationMerger merger = new BiRelationMerger(repository, props);
		EasyMock.replay(repository);
		EasyMock.replay(configRepository);
	}

}
