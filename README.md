# NAA Series

Code for harvesting and analysing series-level data in the National Archives of Australia's RecordSearch dabatase.

See [my research notebook](http://timsherratt.org/research-notebook/projects/hacking-heritage/) for more information.

## Series

Code for harvesting series level descriptions is in `series.py`.

See [Harvesting all NAA series summaries](http://timsherratt.org/research-notebook/notes/naa-series-harvesting/) in my research notebook.

## Functions

Functions are used to model the activities of government. In the CRS system, functions are performed by agencies, and agencies create series. So by following the links between functions, agencies, and series, it should be possible to see how this model of government is reflected in the records described and digitised in RecordSearch.

For some context on the history and use of functions in the National Archives of Australia see ['Natural language searching and government thesauri'](http://webarchive.nla.gov.au/gov/20060912033741/http://e-permanence.gov.au/recordkeeping//gov_online/agift/gov_term/intro.html) by Marian Hoy.

Although you can browse and search for agencies by function in RecordSearch, it's not clear what functions thesaurus is actually in use and how this affects search results. So before following the trail from functions to series, I first need to pull together some data about the functions themselves. The thesauruses created and used by the National Archives seem to have gone through four versions:

* [The CRS Thesaurus](http://recordsearch.naa.gov.au/manual/Provenance/SummaryCRSThes.htm) 
* [AGIFT (Australian Governments' Interactive Functions Thesaurus) version 1 (1999)](http://webarchive.nla.gov.au/gov/20011217173650/http://www.naa.gov.au/recordkeeping/gov_online/agift/summary.html)
* [AGIFT version 2 (2005)](http://webarchive.nla.gov.au/gov/20060914004029/http://naa.gov.au///recordkeeping//gov_online/agift/summary.html)
* [AGIFT version 3 (2015)](http://www.naa.gov.au/agift/)

I've harvested the main terms from each of these versions:

* CRS Thesaurus -- [TXT](data/functions-recordsearch.txt) | [JSON](data/functions-recordsearch.json)
* AGIFT 1  -- [TXT](data/functions-agift1.txt) | [JSON](data/functions-agift1.json)
* AGIFT 2  -- [TXT](data/functions-agift2.txt) | [JSON](data/functions-agift2.json)
* AGIFT 3  -- [TXT](data/functions-agift3.txt) | [JSON](data/functions-agift3.json)

The code used to harvest the functions is in `functions.py`.

AGIFT is made available by the National Archives of Australia under a CC-BY-NC-ND licence.