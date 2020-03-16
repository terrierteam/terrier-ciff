package org.terrier.ciff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

//import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import java.io.File;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.terrier.querying.IndexRef;
import org.terrier.querying.Manager;
import org.terrier.querying.ManagerFactory;
import org.terrier.querying.SearchRequest;
import org.terrier.structures.Index;
import org.terrier.structures.IndexFactory;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;
import org.terrier.tests.ApplicationSetupBasedTest;
import org.terrier.utility.ApplicationSetup;

@EnableRuleMigrationSupport
public class TestIndexIngest extends ApplicationSetupBasedTest
{

    @TempDir
    File anotherTempDir;

    @BeforeEach
    public void dosetup() throws Exception
    {
        super.makeEnvironment();
    }

    @Test public void testItWorks() throws Exception
    {
        ApplicationSetup.TERRIER_INDEX_PATH = anotherTempDir.getAbsolutePath();
        CiffIndexIngest.main(new String[]{
            "resource:/toy-complete-20200309.ciff.gz",
        });

        IndexRef ir = IndexRef.of(ApplicationSetup.TERRIER_INDEX_PATH, ApplicationSetup.TERRIER_INDEX_PREFIX);
        Index index = IndexFactory.of(ir);
        assertNotNull(index);
        for (String struct : new String[] {"document", "inverted", "lexicon"})
        {
            assertTrue("missing " + struct + " structure", index.hasIndexStructure(struct));
        }
        assertNotNull(index.getCollectionStatistics());
        assertNotNull(index.getDocumentIndex());
        assertNotNull(index.getInvertedIndex());
        assertNotNull(index.getLexicon());
        assertNotNull(index.getMetaIndex());       
        
        // IndexOnDisk iod = (IndexOnDisk)index;
        // System.err.println(iod.getProperties());

        assertEquals(14, index.getCollectionStatistics().getNumberOfPointers());
        assertEquals(16, index.getCollectionStatistics().getNumberOfTokens());
        assertEquals(9, index.getCollectionStatistics().getNumberOfUniqueTerms());

        assertEquals(3, index.getCollectionStatistics().getNumberOfDocuments());
        assertEquals("WSJ_1", index.getMetaIndex().getItem("docno", 0));

        LexiconEntry le = index.getLexicon().getLexiconEntry("simpl");
        assertNotNull(le);
        assertEquals(2, le.getFrequency());
        assertEquals(2, le.getDocumentFrequency());
        

        Manager m = ManagerFactory.from(index.getIndexRef());
        SearchRequest srq = m.newSearchRequestFromQuery("content");
        m.runSearchRequest(srq);
        assertEquals(1, srq.getResults().size());
        assertEquals("WSJ_1", srq.getResults().get(0).getAllMetadata()[0]);

        le = index.getLexicon().getLexiconEntry("text");
        IterablePosting ip = index.getInvertedIndex().getPostings(le);
        assertEquals(0, ip.next());
        assertEquals(0, ip.getId());
        assertEquals(1, ip.getFrequency());

        assertEquals(1, ip.next());
        assertEquals(1, ip.getId());
        assertEquals(1, ip.getFrequency());

        assertEquals(2, ip.next());
        assertEquals(2, ip.getId());
        assertEquals(3, ip.getFrequency());
        

        index.close();
    }
}