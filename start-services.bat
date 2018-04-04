START /B qdb\bin\qdbd.exe -a 127.0.0.1:28360 --security=false --transient -r qdb/db
START /B qdb\bin\qdbd.exe -a 127.0.0.1:28361 --security=true --cluster-private-file=cluster-secret-key.txt --user-list=users.txt --transient -r qdb/securedb
