# dbitool

Copyright 2018 Rodrigo O R Antunes <x@rora.com.br>. This software is
distributed under the Apache Licence 2.0. Please see the section LICENCE
below for more information and check the LICENSE file.

## Description

Dbitool links together different data sources and destinations in a tree
structure and manages the data flow thru all modules until EOF has been
reached in all input modules.

Between modules, stream objects are instantiated to buffer data from one
module to another. Different modules and read from the same stream, hence
the tree structure. Data can be read or writen from/to databases, files,
spreadsheets or stdin. But two modules cannot output to the same stream.

Every operation can be logged and errors can be tracked for analysis. The
logging and error are also modules in dbitool and can be included in the
tree structure to be saved in any format handled by the program.

Each module has different arguments to fine tune how data is read or writen.
Some arguments are more global and some are specific to each data format,
source or destination.

Modules are specified in the command line and arguments are separated by a
collon (:). Each argument takes a name and a value separated by equal sign,
for example: fname=text.txt. The first argument is the module class, that
can be listed by running "dbitool --listmodules".

Some modules have to buffer data until EOF has been reached to process the
data. This is generally a pre-requisite of the data format like JSON. In a
JSON file, all rows have to be in memory for the JSON file to be created.
This is why some streaming software use newline delimited JSON (or NDJSON)
to read data from text files, like Apache Spark. Instead of buffering the
whole input file into memory and then parsing the JSON, NDJSON allows each
line of the input file to be read separatelly and streaming of data to start
with the first line, avoiding memory buffering. For big amounts of data,
avoid in memory modules. The XML write module does not use a XML class to
write the data, and therefore is not a "in memory" module. In the future,
jsonwrite module could also use this solution to avoid memory buffers.

## Requirements

Perl 5.006 or later is required to use this program.

## Building and Installation

To build and install dbitool, use:

```
    perl Makefile.PL
    make
    make test
    make install
```

You'll probably need to do the last as root unless you're installing
into a local Perl module tree in your home directory.

## Support

todo...

## Source Repository

dbitool is maintained using git. You can access the current source on
GitHub at:

https://github.com/rorabr/dbitool

or by cloning the repository at:

https://github.com/rorabr/dbitool.git

## License

The dbitool source and acompanying files as a whole is covered by the
following copyright statement and license:

>   Copyright 2018 Rodrigo Otavio Rodrigues Antunes
> 
>   Licensed under the Apache License, Version 2.0 (the "License");
>   you may not use this file except in compliance with the License.
>   You may obtain a copy of the License at
> 
>       http://www.apache.org/licenses/LICENSE-2.0
> 
>   Unless required by applicable law or agreed to in writing, software
>   distributed under the License is distributed on an "AS IS" BASIS,
>   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
>   See the License for the specific language governing permissions and
>   limitations under the License.

Some files in this distribution are individually released under
different licenses, all of which are compatible with the above general
package license but which may require preservation of additional
notices.  All required notices, and detailed information about the
licensing of each file, are recorded in the LICENSE file.

