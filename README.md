# Terrier-CIFF

This allows the [Terrier.org information retrieval platform](http://terrier.org) to ingest files written in the [Common Index File Format](https://github.com/osirrc/ciff/) - see [1]. In doing so, a new Terrier index is written.

## Installation

```shell
    git clone <repo>
    cd terrier-ciff
    mvn install
```

## Usage

The Terier-CIFF package provides a tool for ingesting files written in the Common Index File Format. It can be used directly from the Terrier commandline (version 5.2 minimum).

```shell
    cd /path/to/terrier
    bin/terrier  -Dterrier.mvn.coords=org.terrier:terrier-ciff:0.2 ciff-ingest /path/to/ciff.gz
```

This creates a new index at the default location. The standard `-I` option can be used to change the location of the generated index.

## Additional Weighting Models

We provide variants of BM25 that are aligned with those in the Anserini platform. These are as follows:
 - `BM25_log10` - This follows Terrier's standard implementation, but uses log base 10 instead of log base 2.
 - `BM25_log10_nonum` - This removes the unnecessary (k+1) from the BM25 numerator. It alsos uses log base 10.

## Implementation Notes

 - collection statistics (number of tokens/average document length) are calculated using the document lengths file; we do not consider the collection statistics in the header.
 - we use a shaded version of protobuf 3, as Terrier depends on Hadoop, which includes protobuf 2.5

## Credits

Craig Macdonald, University of Glasgow

## References

[1] Under review.

[2] Which BM25 Do You Mean? A Large-Scale Reproducibility Study of Scoring Variants. Chris Kamphuis, Arjen de Vries, Leonid Boytsov and Jimmy Lin. In Proceedings of ECIR 2020.
