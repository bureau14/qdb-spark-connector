# qdb-spark-connector
Official quasardb Spark connector.

# Tests

In order to run the tests, please download the latest quasardb-server and extract in a `qdb` subdirectory like this:

```mkdir qdb
cd qdb
wget https:////download.quasardb.net/quasardb/2.1/2.1.0-beta.1/server/qdb-2.1.0master-darwin-64bit-server.tar.gz
tar -xzf qdb-2.1.0master-darwin-64bit-server.tar.gz```

Then launch the integration test using sbt:

```sbt test```

# A note for OSX users

QuasarDB uses a C++ standard library that is not shipped with OSX by default. Unfortunately, due to static linking restrictions on OSX this can cause runtime errors such like these:

```dyld: Symbol not found: __ZTISt18bad_variant_access```

Until a fix is available for the QuasarDB client libraries, the best course of action is to download the llvm libraries and tell your shell where to find them:

```cd qdb
wget http://releases.llvm.org/5.0.0/clang+llvm-5.0.0-x86_64-apple-darwin.tar.xz
tar -xzf clang+llvm-5.0.0-x86_64-apple-darwin.tar.xz```

And then run the tests like this:

```LD_LIBRARY_PATH=qdb/clang+llvm-5.0.0-x86_64-apple-darwin/lib/ sbt test```
