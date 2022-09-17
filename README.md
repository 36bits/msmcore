# msmquote

**msmquote** is a Java command-line application for updating current and historical security quotes and current currency exchange rates in Microsoft Money files, using a variety of online and offline quote sources.

Quote data can be updated for any security or currency where a quote is available from these supported sources:

* The [Yahoo Finance](https://finance.yahoo.com) quote data API
* A Google Sheets spreadsheet with quote data provided by the [GOOGLEFINANCE](https://support.google.com/docs/answer/3093281) security information function

Historical data for a single security can be updated where a quote is available from the following sources:

* The [Yahoo Finance](https://finance.yahoo.com) historical quote data API
* A CSV file generated by the Yahoo Finance historical quote data download facility
* A Google Sheets spreadsheet with quote data provided by the [GOOGLEFINANCE](https://support.google.com/docs/answer/3093281) security information function

# Documentation
Please see the [wiki](https://github.com/36bits/msmquote/wiki) for full details of how to use **msmquote**.

# Author
Jonathan Casiot ([e-mail](mailto:msmquote@pueblo.co.uk))

# Licence
This project is licenced under the GNU GPL Version 3 (see the [LICENCE](./LICENSE) file).

# With Thanks To
* Hung Le for Sunriise, the original Microsoft Money quote updater
* Yahoo Finance for a decent securities quote API