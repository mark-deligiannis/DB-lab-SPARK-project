Queries are implemented in files with with name: Q{query number}_{method}.py.
Queries can be run using the following command (from the appropriate folder):
```
spark-submit --num-executors {number of executors} {python executable}.py
```
*Note that schema files may not be run on their own.*
