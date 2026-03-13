libstemmer_c

Sources:

- https://github.com/snowballstem/snowball
- https://snowballstem.org/dist/libstemmer_c-3.0.1.tar.gz
- https://snowballstem.org/download.html

## Update steps:

1. Go to the submodule directory

```Bash
cd contrib/libstemmer_c/
```

1. Create a new branch for the new version:

```Bash
git checkout -b "Clickhouse/Clickhouse/release/vX.X.X"
```

2. Get the tarball (in a different location) using one of the following methods:

### Download distribution tarball

```Bash
wget https://snowballstem.org/dist/libstemmer_c-X.X.X.tar.gz
```

### Generate tarball from sources:

```Bash
git clone https://github.com/snowballstem/snowball
cd snowball
make dist_libstemmer_c
```

3. Uncompress and override

```Bash
tar --overwrite -xvzf libstemmer_c-X.X.X.tar.gz -C /path/to/this/repo/clone
cd /path/to/this/repo/clone
```

4. Commit the changes

```Bash 
git add .
git commit -m "Snapshot to version vX.X.X"
git push --set-upstream origin Clickhouse/release/vX.X.X
```

5. Regenerate the `CMakeLists.txt` in THIS directory:

(suggested steps, run from the repository root)

```Bash
cd /path/to/this/repo/clone

SOURCES=$(find contrib/libstemmer_c -name "*.c" -not -path "*/examples/*" | sort | sed 's|contrib/libstemmer_c/|    ${LIBRARY_DIR}/|')

cat > contrib/libstemmer-c-cmake/CMakeLists.txt << EOF

set(LIBRARY_DIR "\${ClickHouse_SOURCE_DIR}/contrib/libstemmer_c")

add_library(_stemmer
${SOURCES}
)

target_include_directories(_stemmer SYSTEM PUBLIC "\${LIBRARY_DIR}/include")
add_library(ch_contrib::stemmer ALIAS _stemmer)
EOF

```
