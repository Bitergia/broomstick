# Broomstick

Broomstick provides a set of functions to compute metrics based on
GrimoireLab data.

It tries to de-couple the implementation of the metrics and the data access
layer. GrimoireLab data model is currently based on ElasticSearch indexes,
so it is the current data access layer. On top of this, Pandas data frames are
used to store and return the data.

A basic view of Broomstick components could be:
* `broomstick/data`: data access layer. Returns basic data types and Pandas
  data frames.
* `broomstick/metrics`: where the metrics are implemented.
* `test`: self-explanatory, the tests.
* `notebooks`: Jupyter notebooks to implement different use cases based on
  the metrics provided by Broomstick. They can be seen as specific reports.
  They are not part of Broomstick by themselves, nevertheless they can be
  considered a front-end for the Broomstick metrics.

 