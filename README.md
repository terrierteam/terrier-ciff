# Terrier-CIFF

This ingests files written in the [Common Index File Format](https://github.com/osirrc/ciff/).

## Installation

```shell
    git clone <repo>
    cd terrier-ciff
    mvn install
```

## Usage

The Terier-CIFF package provides a tool for ingesting files written in the Common Index File Format. It can be used directly from the Terrier commandline.

```shell
    cd /path/to/terrier
    bin/terrier  -Dterrier.mvn.coords=org.terrier:terrier-ciff:0.2 ciff-ingest /path/to/ciff.gz
```

## See also

## Implementation Notes

 - collection statistics (number of tokens/average document length) are calculated using the document lengths file; we do not consider the collection statistics in the header.
 - we use a shaded version of protobuf 3, as Terrier depends on Hadoop, which includes protobuf 2.5

## Credits

Craig Macdonald, University of Glasgow
