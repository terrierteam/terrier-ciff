
package org.terrier.ciff;

import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.terrier.applications.CLITool;
import org.terrier.structures.AbstractPostingOutputStream;
import org.terrier.structures.BasicLexiconEntry;
import org.terrier.structures.BitIndexPointer;
import org.terrier.structures.DocumentIndexEntry;
import org.terrier.structures.FSOMapFileLexiconOutputStream;
import org.terrier.structures.FieldLexiconEntry;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.LexiconOutputStream;
import org.terrier.structures.SimpleDocumentIndexEntry;
import org.terrier.structures.indexing.CompressingMetaIndexBuilder;
import org.terrier.structures.indexing.CompressionFactory;
import org.terrier.structures.indexing.DocumentIndexBuilder;
import org.terrier.structures.indexing.FSOMapFileLexiconUtilities;
import org.terrier.structures.indexing.LexiconBuilder.BasicLexiconCollectionStaticticsCounter;
import org.terrier.structures.indexing.LexiconBuilder.CollectionStatisticsCounter;
import org.terrier.structures.indexing.MetaIndexBuilder;
import org.terrier.structures.postings.BasicPostingImpl;
import org.terrier.structures.postings.Posting;
import org.terrier.structures.postings.bit.BasicIterablePosting;
import org.terrier.utility.ApplicationSetup;
import org.terrier.utility.Files;
import org.terrier.utility.TerrierTimer;

import io.osirrc.ciff.CommonIndexFileFormat;

public class CiffIndexIngest {

    public static class Command extends CLITool.CLIParsedCLITool {

        @Override
        public int run(CommandLine line) throws Exception {
            String[] args = line.getArgs();
            if (args.length !=1 ) {
                System.err.println("Usage: " + this.commandname() + " /path/to/ciff.gz");
                return -1;
            }
            CiffIndexIngest.main(args);
            return 0;
        }

        @Override
        public String commandname() {
            return "ciff-ingest";
        }

        @Override
        public String helpsummary() {
            return "builds a Terrier index from a CIFF file";
        }

        @Override
        public String sourcepackage() {
            return "ciff";
        }

    }

    protected String basicInvertedIndexPostingIteratorClass = BasicIterablePosting.class.getName();

    public static void main(final String[] args) throws Exception {
        final String ciffFile = args[0];
        
        final InputStream fileIn = Files.openFileStream(ciffFile);

        final IndexOnDisk index = Index.createNewIndex(ApplicationSetup.TERRIER_INDEX_PATH,
                ApplicationSetup.TERRIER_INDEX_PREFIX);

        CommonIndexFileFormat.Header header = CommonIndexFileFormat.Header.parseDelimitedFrom(fileIn);
        
        index.setIndexProperty("max.term.length", "20");

        int numFields = 0;
        final LexiconOutputStream<String> lexStream = new FSOMapFileLexiconOutputStream(index, "lexicon",
                (numFields > 0 ? FieldLexiconEntry.Factory.class : BasicLexiconEntry.Factory.class));
        final CompressionFactory.CompressionConfiguration invertedCompression = CompressionFactory
                .getCompressionConfiguration("inverted", new String[0], 0, 0);

        final BasicLexiconEntry.Factory lexEntryF = new BasicLexiconEntry.Factory();
        final AbstractPostingOutputStream pos = invertedCompression.getPostingOutputStream(index.getPath() + "/"
                + index.getPrefix() + "." + "inverted" + invertedCompression.getStructureFileExtension());


        TerrierTimer tt = new TerrierTimer("Constructing inverted posting list", header.getNumPostingsLists());
        try{
            tt.start();
            for(int termid=0; termid < header.getNumPostingsLists();termid++)
            {
                CommonIndexFileFormat.PostingsList pl = CommonIndexFileFormat.PostingsList.parseDelimitedFrom(fileIn);
                final LexiconEntry lee = lexEntryF.newInstance();
                lee.setDocumentFrequency((int) pl.getDf());
                lee.setFrequency((int) pl.getCf());
                lee.setTermId(termid);

                // we perform two passes on the posting list, one to get maxtf
                lee.setMaxFrequencyInDocuments(pl.getPostingsList().stream().map(p -> p.getTf()).reduce(Integer::max).get());

                Iterator<Posting> iterOut = new Iterator<Posting>() {
                    int offset = -1;
                    int prev = 0;
                    @Override
                    public boolean hasNext() {
                        return pl.getPostingsCount() > 0 && offset < pl.getPostingsCount() -1;
                    }

                    @Override
                    public Posting next() {
                        offset++;
                        CommonIndexFileFormat.Posting pIn = pl.getPostings(offset);
                        prev += pIn.getDocid(); //the docids in the postings lists are d-gapped
                        return new BasicPostingImpl(prev, pIn.getTf());
                    }

                };
                final BitIndexPointer pointer = pos.writePostings(iterOut, -1);
                lee.setPointer(pointer);

                lexStream.writeNextEntry(pl.getTerm(), lee);
                tt.increment();
            }
        } finally {
            tt.finished();
        }
        pos.close();
        invertedCompression.writeIndexProperties(index, "lexicon-entry-inputstream");
        lexStream.close();
        final CollectionStatisticsCounter css = new BasicLexiconCollectionStaticticsCounter(index);
        FSOMapFileLexiconUtilities.optimise("lexicon", index, css);
        css.close();

        readCombinedDocumentFile(fileIn, header.getNumDocs(), index);   
        index.setIndexProperty("num.Documents", String.valueOf(header.getTotalDocs()));
        index.setIndexProperty("num.Terms", String.valueOf(header.getTotalPostingsLists()));
        index.setIndexProperty("num.Tokens", String.valueOf(header.getTotalTermsInCollection()));
        
        fileIn.close();

        index.flush();
        System.out.println("Finished ingesting "+ciffFile+" ("+index.getCollectionStatistics().getNumberOfDocuments()+ " documents)");
        System.out.println("New index at " + index.getIndexRef().toString());
        index.close();
    }

    

    protected static void readCombinedDocumentFile(InputStream fileIn, int numDocs, IndexOnDisk index) throws Exception {
        final MetaIndexBuilder mib = createMetaIndexBuilder(index);
        final DocumentIndexBuilder dib = new DocumentIndexBuilder(index, "document");

        final DocumentIndexEntry die = new SimpleDocumentIndexEntry();
        long numtokens = 0;
        index.addIndexStructure("document-factory", SimpleDocumentIndexEntry.Factory.class.getName(), "", "");

        TerrierTimer tt = new TerrierTimer("Constructing document metadata", numDocs);
        try{
            tt.start();
            for (int i=0; i<numDocs; i++) {
                CommonIndexFileFormat.DocRecord docRecord = CommonIndexFileFormat.DocRecord.parseDelimitedFrom(fileIn);
                final String docno = docRecord.getCollectionDocid();
                final int doclength = docRecord.getDoclength();
                die.setDocumentLength(doclength);
                dib.addEntryToBuffer(die);
                mib.writeDocumentEntry(new String[]{docno});
                numtokens += doclength;
                tt.increment();
            }
        } finally {
            tt.finished();
        }

        index.setIndexProperty("num.Tokens", ""+numtokens);
        dib.finishedCollections();
        dib.close();
        mib.close();
        index.flush();
    }

    protected static MetaIndexBuilder createMetaIndexBuilder(final IndexOnDisk currentIndex)
	{
		final String[] forwardMetaKeys = new String[]{"docno"};
		final int[] metaKeyLengths = new int[]{100};
		final String[] reverseMetaKeys = new String[]{"docno"};
		return new CompressingMetaIndexBuilder(currentIndex, forwardMetaKeys, metaKeyLengths, reverseMetaKeys);
	}

}