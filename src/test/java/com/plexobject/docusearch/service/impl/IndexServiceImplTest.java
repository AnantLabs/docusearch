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

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.docs.DocumentsDatabaseIndexer;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.domain.DocumentBuilder;
import com.plexobject.docusearch.http.RestClient;
import com.plexobject.docusearch.index.IndexPolicy;
import com.plexobject.docusearch.persistence.ConfigurationRepository;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.PagedList;
import com.plexobject.docusearch.persistence.RepositoryFactory;
import com.plexobject.docusearch.service.IndexService;

public class IndexServiceImplTest {
    private static final String PUBLICATION_ID = "publication_id";
    private static final String COMPANY_ID = "company_id";
    private static final String COMPANIES_PUBLICATIONS = "companies_publications";
    private static final String PUBLICATIONS = "publications";
    private static final String COMPANIES = "test_companies";
    private static final String DB_NAME = "MYDB";
    private static final int LIMIT = Configuration.getInstance().getPageSize();

    private static Logger LOGGER = Logger.getRootLogger();

    private DocumentRepository repository;
    private ConfigurationRepository configRepository;
    private DocumentsDatabaseIndexer indexer;
    private int idCount;
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
        service = new IndexServiceImpl(indexer);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public final void testCreate() {
        EasyMock.expect(
                repository.getAllDocuments(COMPANIES, null, null, LIMIT + 1))
                .andReturn(newDocuments());

        EasyMock.expect(configRepository.getIndexPolicy(COMPANIES)).andReturn(
                newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.createIndexUsingPrimaryDatabase(COMPANIES);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(RestClient.OK_CREATED, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains("rebuilt index for " + COMPANIES));
    }

    @Test
    public final void testCreateSecondaryWithNullIndexer() {
        Response response = service.createIndexUsingSecondaryDatabase(null,
                PUBLICATIONS, COMPANIES_PUBLICATIONS, COMPANY_ID,
                PUBLICATION_ID);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
        response = service.createIndexUsingSecondaryDatabase(COMPANIES, null,
                COMPANIES_PUBLICATIONS, COMPANY_ID, PUBLICATION_ID);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
        response = service.createIndexUsingSecondaryDatabase(COMPANIES,
                PUBLICATIONS, null, COMPANY_ID, PUBLICATION_ID);
        Assert.assertEquals(RestClient.CLIENT_ERROR_BAD_REQUEST, response
                .getStatus());
    }

    @Test
    public final void testCreateSecondary() {
        EasyMock.expect(
                repository.getAllDocuments(COMPANIES_PUBLICATIONS, null, null,
                        LIMIT + 1)).andReturn(newDocuments());

        EasyMock.expect(repository.getDocument(PUBLICATIONS, "2")).andReturn(
                newDocument());

        EasyMock
                .expect(
                        configRepository.getIndexPolicy(COMPANIES + "_"
                                + PUBLICATIONS)).andReturn(newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.createIndexUsingSecondaryDatabase(
                COMPANIES, PUBLICATIONS, COMPANIES_PUBLICATIONS, COMPANY_ID,
                PUBLICATION_ID);
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(RestClient.OK_CREATED, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains("rebuilt index for " + COMPANIES));
    }

    @Test
    public final void testUpdate() {
        EasyMock.expect(repository.getDocument(COMPANIES, "id")).andReturn(
                newDocument());
        EasyMock.expect(configRepository.getIndexPolicy(COMPANIES)).andReturn(
                newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.updateIndexUsingPrimaryDatabase(COMPANIES,
                "id");
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains(
                        "updated 1 documents in index for " + COMPANIES
                                + " with ids id"));
    }

    @Test
    public final void testUpdateSecondary() {
        EasyMock.expect(
                repository.getAllDocuments(COMPANIES_PUBLICATIONS, null, null,
                        LIMIT + 1)).andReturn(newDocuments());
        EasyMock.expect(repository.getDocument(PUBLICATIONS, "2")).andReturn(
                newDocument());
        EasyMock.expect(
                configRepository.getIndexPolicy(COMPANIES + PUBLICATIONS))
                .andReturn(newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.updateIndexUsingSecondaryDatabase(
                COMPANIES, PUBLICATIONS, COMPANIES_PUBLICATIONS, COMPANY_ID,
                PUBLICATION_ID, "2");
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains(
                        "updated 1 documents in index for " + COMPANIES
                                + " with ids 2"));
    }

    @Test
    public final void testUpdateSecondaryNotFound() {
        EasyMock.expect(
                repository.getAllDocuments(COMPANIES_PUBLICATIONS, null, null,
                        LIMIT + 1)).andReturn(newDocuments());

        EasyMock.expect(
                configRepository.getIndexPolicy(COMPANIES + PUBLICATIONS))
                .andReturn(newIndexPolicy());

        EasyMock.replay(repository);
        EasyMock.replay(configRepository);

        Response response = service.updateIndexUsingSecondaryDatabase(
                COMPANIES, PUBLICATIONS, COMPANIES_PUBLICATIONS, COMPANY_ID,
                PUBLICATION_ID, "0");
        EasyMock.verify(repository);
        EasyMock.verify(configRepository);
        Assert.assertEquals(200, response.getStatus());
        Assert.assertTrue("unexpected response "
                + response.getEntity().toString(), response.getEntity()
                .toString().contains(
                        "updated 0 documents in index for " + COMPANIES));
    }

    private PagedList<Document> newDocuments() {
        return PagedList.asList(newDocument());

    }

    @SuppressWarnings("unchecked")
    private Document newDocument() {
        final Map<String, String> map = new TreeMap<String, String>();
        map.put("Y", "8");
        map.put("Z", "9");
        final List<? extends Object> arr = Arrays.asList(11, "12", 13, "14");
        final Document doc = new DocumentBuilder(DB_NAME).setId(
                String.valueOf(++idCount)).put("company_id", "1").put(
                "publication_id", "2").put("C", map).put("D", arr).build();

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
