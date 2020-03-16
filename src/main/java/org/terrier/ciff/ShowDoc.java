package org.terrier.ciff;

import org.apache.commons.cli.CommandLine;
import org.terrier.applications.CLITool.CLIParsedCLITool;
import org.terrier.structures.Index;
import org.terrier.structures.IndexFactory;
import org.terrier.structures.Lexicon;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.MetaIndex;
import org.terrier.structures.PostingIndex;
import org.terrier.structures.postings.IterablePosting;
import org.terrier.terms.PorterStemmer;

/** Useful utility that gives the frequency of each document containing given term(s) */
public class ShowDoc extends CLIParsedCLITool {

    @Override
    public int run(CommandLine line) throws Exception {
        Index index = IndexFactory.of(super.getIndexRef(line));

        PostingIndex<?> inv = index.getInvertedIndex();
        MetaIndex meta = index.getMetaIndex();
        Lexicon<String> lex = index.getLexicon();
        //PorterStemmer stemmer = new PorterStemmer();
        
        for(String t : line.getArgs())
        {
            //t = stemmer.stem(t);
            LexiconEntry le = lex.getLexiconEntry( t);
            if (le == null)
            {
                System.err.println("term " + t + " not found");
                continue;
            }
            System.err.println("term " + t +" " + le.toString());
            IterablePosting postings = inv.getPostings(le);

            while (postings.next() != IterablePosting.EOL) {
                String docno = meta.getItem("docno", postings.getId());
                System.out.println(docno + " term "+ t +" with  frequency " + postings.getFrequency()  + " doclen " + postings.getDocumentLength());
            }
        }
        return 0;
    }

}