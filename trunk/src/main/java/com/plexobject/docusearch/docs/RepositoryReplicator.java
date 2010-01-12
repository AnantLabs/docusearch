package com.plexobject.docusearch.docs;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.plexobject.docusearch.Configuration;
import com.plexobject.docusearch.domain.Document;
import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;
import com.plexobject.docusearch.persistence.DocumentRepository;
import com.plexobject.docusearch.persistence.DocumentsIterator;
import com.plexobject.docusearch.persistence.PersistenceException;
import com.plexobject.docusearch.persistence.couchdb.DocumentRepositoryCouchdb;
import com.plexobject.docusearch.persistence.file.DocumentRepositoryImpl;

public class RepositoryReplicator {
    private static final Logger LOGGER = Logger
            .getLogger(RepositoryReplicator.class);
    private final DocumentRepository srcRepository;
    private final DocumentRepository dstRepository;

    public RepositoryReplicator(DocumentRepository srcRepository,
            DocumentRepository dstRepository) {
        this.srcRepository = srcRepository;
        this.dstRepository = dstRepository;
    }

    public void copy(final String db) {
        try {
            final Timer timer = Metric.newTimer(getClass().getSimpleName()
                    + ".run");
            int total = 0;
            final Iterator<List<Document>> docsIt = new DocumentsIterator(
                    srcRepository, db, Configuration.getInstance()
                            .getPageSize());
            while (docsIt.hasNext()) {
                List<Document> sourceDocuments = docsIt.next();
                total += sourceDocuments.size();
                for (Document sourceDocument : sourceDocuments) {
                    dstRepository.saveDocument(sourceDocument, true);
                }
            }
            timer.stop("Copied " + total + " records of " + db + " from "
                    + srcRepository + " to " + dstRepository);

        } catch (PersistenceException e) {
            LOGGER.error("Failed to copy " + db, e);
        }
    }

    public static void main(String[] args) {
        for (String db : args) {
            LOGGER.info("Copying " + db);
            final DocumentRepository srcRepository = new DocumentRepositoryImpl();
            final DocumentRepository dstRepository = new DocumentRepositoryCouchdb();

            RepositoryReplicator repositoryReplicator = new RepositoryReplicator(
                    srcRepository, dstRepository);
            repositoryReplicator.copy(db);
        }
    }
}
