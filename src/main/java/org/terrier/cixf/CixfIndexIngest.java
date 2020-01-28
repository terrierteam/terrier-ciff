

package org.terrier.cixf;

import java.io.BufferedReader;
import java.io.InputStream;

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

import io.anserini.cidxf.CommonIndexFormat;

public class CixfIndexIngest {

    public static class Command extends CLITool.CLIParsedCLITool {

        @Override
        public int run(CommandLine line) throws Exception {
            String[] args = line.getArgs();
            if (args.length != 3 && args.length != 2) {
                System.err.println("Usage: " + this.commandname() + " postingsFile doclenFile docnoFile");
                System.err.println("Usage: " + this.commandname() + " postingsFile doclen_and_docnoFile");
                return -1;
            }
            CixfIndexIngest.main(args);
            return 0;
        }

        @Override
        public String commandname() {
            return "cixf-ingest";
        }

        @Override
        public String helpsummary() {
            return "builds a Terrier index from CIXF files";
        }

        @Override
        public String sourcepackage() {
            return "cixf";
        }

    }

    protected String basicInvertedIndexPostingIteratorClass = BasicIterablePosting.class.getName();



    public static void main(final String[] args) throws Exception {
        final String postingsFile = args[0];
        final String doclenFile = args[1];
        final String docnoFile = args.length > 2 ? args[2] : null;

        final InputStream is = Files.openFileStream(postingsFile);

        final IndexOnDisk index = Index.createNewIndex(ApplicationSetup.TERRIER_INDEX_PATH, ApplicationSetup.TERRIER_INDEX_PREFIX);
        
        index.setIndexProperty("max.term.length", "20");

        int numFields = 0;
        final LexiconOutputStream<String> lexStream = new FSOMapFileLexiconOutputStream(index, "lexicon",
                (numFields > 0 ? FieldLexiconEntry.Factory.class : BasicLexiconEntry.Factory.class));
        final CompressionFactory.CompressionConfiguration invertedCompression = CompressionFactory
                .getCompressionConfiguration("inverted", new String[0], 0, 0);

        final BasicLexiconEntry.Factory lexEntryF = new BasicLexiconEntry.Factory();
        final AbstractPostingOutputStream pos = invertedCompression.getPostingOutputStream(index.getPath() + "/"
                + index.getPrefix() + "." + "inverted" + invertedCompression.getStructureFileExtension());

        CommonIndexFormat.PostingsList pl = null;
        int termid=0;
        while ((pl = CommonIndexFormat.PostingsList.parseDelimitedFrom(is)) != null) {

            final LexiconEntry lee = lexEntryF.newInstance();
            lee.setDocumentFrequency((int) pl.getDf());
            lee.setFrequency((int) pl.getCf());
            lee.setTermId(termid++);

            // we perform two passes on the posting list, one to get maxtf
            lee.setMaxFrequencyInDocuments(pl.getPostingList().stream().map(p -> p.getTf()).reduce(Integer::max).get());

            final BitIndexPointer pointer = 
                pos.writePostings(pl.getPostingList().stream()
                    .map(p -> (Posting) new BasicPostingImpl(p.getDocid(), p.getTf())).iterator(), -1);
            lee.setPointer(pointer);

            lexStream.writeNextEntry(pl.getTerm(), lee);
        }
        pos.close();
        is.close();
        invertedCompression.writeIndexProperties(index, "lexicon-entry-inputstream");
        lexStream.close();
        final CollectionStatisticsCounter css = new BasicLexiconCollectionStaticticsCounter(index);
        FSOMapFileLexiconUtilities.optimise("lexicon", index, css);
        css.close();

        long tokens = 0;
        if (docnoFile != null)
        {
            tokens = readDoclenFile(doclenFile, index);
            readDocnoFile(docnoFile, index);
        }
        else
        {
            tokens = readCombinedFile(doclenFile, index);
        }
        index.setIndexProperty("num.Tokens", ""+tokens);
        index.flush();
        index.close();
    }

    protected static long readDoclenFile(String inputfile, IndexOnDisk index) throws Exception {
        final DocumentIndexBuilder dib = new DocumentIndexBuilder(index, "document");
        BufferedReader br = Files.openFileReader(inputfile);
        String line = null;
        final DocumentIndexEntry die = new SimpleDocumentIndexEntry();
        index.addIndexStructure("document-factory", SimpleDocumentIndexEntry.Factory.class.getName(), "", "");
        long numtokens = 0;
        while ((line = br.readLine()) != null) {
            final int doclength = Integer.parseInt(line.split("\t", 2)[1]);
            die.setDocumentLength(doclength);
            dib.addEntryToBuffer(die);
            numtokens += doclength;
        }
        dib.finishedCollections();
        dib.close();
        br.close();
        return numtokens;
    }

    protected static void readDocnoFile(String inputfile, IndexOnDisk index) throws Exception {

        String line = null;
        final MetaIndexBuilder mib = createMetaIndexBuilder(index);
        BufferedReader br = Files.openFileReader(inputfile);
        while( (line = br.readLine()) != null ) {
            line = line.trim();
            mib.writeDocumentEntry(new String[]{line});
        }
        mib.close();
        index.flush();
    }

    protected static long readCombinedFile(String inputfile, IndexOnDisk index) throws Exception {
        final MetaIndexBuilder mib = createMetaIndexBuilder(index);
        final DocumentIndexBuilder dib = new DocumentIndexBuilder(index, "document");

        BufferedReader br = Files.openFileReader(inputfile);
        String line = null;
        final DocumentIndexEntry die = new SimpleDocumentIndexEntry();
        long numtokens = 0;
        index.addIndexStructure("document-factory", SimpleDocumentIndexEntry.Factory.class.getName(), "", "");
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t", 3);
            final String docno = parts[1];
            final int doclength = Integer.parseInt(parts[2]);
            die.setDocumentLength(doclength);
            dib.addEntryToBuffer(die);
            mib.writeDocumentEntry(new String[]{docno});
            numtokens += doclength;
        }
        dib.finishedCollections();
        dib.close();
        mib.close();
        index.flush();
        return numtokens;
    }

    protected static MetaIndexBuilder createMetaIndexBuilder(final IndexOnDisk currentIndex)
	{
		final String[] forwardMetaKeys = new String[]{"docno"};
		final int[] metaKeyLengths = new int[]{100};
		final String[] reverseMetaKeys = new String[]{"docno"};
		return new CompressingMetaIndexBuilder(currentIndex, forwardMetaKeys, metaKeyLengths, reverseMetaKeys);
	}

}