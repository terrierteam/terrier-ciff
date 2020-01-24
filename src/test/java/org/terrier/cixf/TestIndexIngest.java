package org.terrier.cixf;

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
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
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
        CixfIndexIngest.main(new String[]{
            "resource:/micro.postings",
            "resource:/micro.doclengths",
            "resource:/micro.docids"
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

        assertTrue(index.getCollectionStatistics().getNumberOfPointers() > 0);
        assertTrue(index.getCollectionStatistics().getNumberOfTokens() > 0);
        assertTrue(index.getCollectionStatistics().getNumberOfUniqueTerms() > 0);

        assertEquals(6, index.getCollectionStatistics().getNumberOfDocuments());
        assertEquals("FT911-3323", index.getMetaIndex().getItem("docno", 0));

        LexiconEntry le = index.getLexicon().getLexiconEntry("forecast");
        assertNotNull(le);
        assertEquals(5, le.getFrequency());
        assertEquals(4, le.getDocumentFrequency());
        

        Manager m = ManagerFactory.from(index.getIndexRef());
        SearchRequest srq = m.newSearchRequestFromQuery("french");
        m.runSearchRequest(srq);
        assertEquals(1, srq.getResults().size());
        assertEquals("FT911-3326", srq.getResults().get(0).getAllMetadata()[0]);
        index.close();
    }
}