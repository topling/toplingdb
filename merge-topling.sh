if ! test -z "`git status --porcelain -uno`"; then
  echo Workspace is not clean
  git status --porcelain -uno
  exit 1
fi

git reset --hard topling-FRocksDB-8.10.0
rm -rf *
rm -f .github/workflows/topling-jni.yml .gitmodules
git checkout .
git merge memtable_as_log_index
