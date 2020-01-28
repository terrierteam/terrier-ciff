# Terrier-CIXF

This ingests files written in the Common IndeXing Format.

## Installation

```shell
    git clone <repo>
    cd terrier-cixf
    mvn install
```

## Usage

The Terier-CIXF package provides a tool for ingesting files written in the Common IndeXing Format. It can be used directly from the Terrier commandline.

```shell
    cd /path/to/terrier
    bin/terrier  -Dterrier.mvn.coords=org.terrier:terrier-cixf:0.0.1-SNAPSHOT cixf-ingest micro.postings micro.docs
```

## See also

## Implementation Notes

 - collection statistics (number of tokens/average document length) are calculated using the document lengths file, as the postings file may not contain all terms/postings
 - we use a shaded version of protobuf 3, as Terrier depends on Hadoop, which includes protobuf 2.5

## Credits

Craig Macdonald, University of Glasgow
