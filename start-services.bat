START /B qdb\bin\qdbd.exe -a 127.0.0.1:28360 --security=false --storage-engine=transient
START /B qdb\bin\qdbd.exe -a 127.0.0.1:28362 --security=true --cluster-private-file=cluster-secret-key.txt --user-list=users.txt --storage-engine=transient

