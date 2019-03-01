Singapore Land Ownership Info
=============================

This is a dump of all Singapore Land Ownership info, retrived from Onemap.sg.
The download script is attached, and can run it to generate the dump, the dump file size will be around 500MB.

The Singapore postal codes used here is based on [xkjyeah's](https://github.com/xkjyeah/singapore-postal-codes) work.

Note: Use of the data is governed by the [Open Data Licence](https://www.onemap.sg/legal/opendatalicence.html)


After you have the dump file generated, can then use `grep` to do the search, eg.:
```
grep "Housing" data-result-landlots.json >> search-result-hdb.txt
```
