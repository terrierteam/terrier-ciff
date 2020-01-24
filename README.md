# Terrier-CIXF

This ingests files written in the Common IndeXing Format.

## Installation

```shell
    git clone <repo>
    cd terrier-cixf
    mvn install
```

## Usage

The Terier-CIXF package provides a tool for ingesting files written in the Common IndeXing Format. It can be used directly from the Terrier commandline.

```shell
    cd /path/to/terrier
    bin/terrier  -Dterrier.mvn.coords=org.terrier:terrier-cixf:0.0.1-SNAPSHOT cixf-ingest micro.postings micro.doclens micro.docids
```

## See also

TODO

## Credits

Craig Macdonald, University of Glasgow