# mysql-percentile
user-defined aggregate function to calculate percentiles in a MySQL query, similar to min/max/avg.

Compile with:
gcc -fPIC -Wall -I /usr/include/mysql51/mysql/ -shared -o udf_percentile.so  udf_percentile.cc
(to get the include files, you need to install the mysql-devel package, as in "yum install mysql-devel")

Copy to the plugin directory of MySQL (SHOW VARIABLES LIKE 'plugin_dir')
cp udf_percentile.so /usr/lib64/mysql/plugin

Register the function:
CREATE AGGREGATE FUNCTION percentile RETURNS REAL SONAME 'udf_percentile.so'

Use the function:
SELECT percentile(loadTime, 99) FROM user_data GROUP BY WEEK(`timestamp`)
