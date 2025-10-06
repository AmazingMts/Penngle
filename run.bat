::java -cp "classes" cis5550/test/HW5Test

::java -cp "classes" cis5550/kvs/Coordinator 8000

::java -cp "classes" cis5550/kvs/Worker 8001 C:\Study\Fall2024\CIS555\test\Fa24-CIS5550-Project-CyberSquad\__worker localhost:8000

::
::java -cp "classes" cis5550.kvs.Coordinator 8000

::
start cmd /k java -cp "classes;integrated/lib/lucene-analysis-common-10.0.0.jar;integrated/lib/lucene-core-10.0.0.jar" cis5550.kvs.Coordinator 8000
start cmd /k java -cp "classes;integrated/lib/lucene-analysis-common-10.0.0.jar;integrated/lib/lucene-core-10.0.0.jar" cis5550.kvs.Worker 8001 worker1 localhost:8000
start cmd /k java -cp "classes;integrated/lib/lucene-analysis-common-10.0.0.jar;integrated/lib/lucene-core-10.0.0.jar" cis5550.flame.Coordinator 9000 localhost:8000
start cmd /k java -cp "classes;integrated/lib/lucene-analysis-common-10.0.0.jar;integrated/lib/lucene-core-10.0.0.jar" cis5550.flame.Worker 9002 localhost:9000
start cmd /k java -cp "classes" cis5550.jobs.Query 8100 localhost:8000
start cmd /k java -cp "classes" cis5550.frontend.Frontend 8080 localhost:8100
::java -cp classes cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler http://simple.crawltest.401.cis5550.net