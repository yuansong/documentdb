#!/bin/bash
# apply patches (idempotent), commit series, configure & build PG16-shm
set -e
cd /opt/pg16shm-src

G="git -c user.name=yuanyuansong -c user.email=yuanyuansong@local"

# 1) new kernel files (fresh copy is harmless)
cp /home/ysyuan/shm_channel.h  src/include/storage/shm_channel.h
cp /home/ysyuan/shm_channel.c  src/backend/storage/ipc/shm_channel.c

# 2) apply integration patches (idempotent)
python3 /home/ysyuan/shm_channel_patch.py

# 3) commit series (skip when nothing staged)
git add src/include/storage/shm_channel.h src/backend/storage/ipc/shm_channel.c src/backend/storage/ipc/Makefile
if ! git diff --cached --quiet; then
  $G commit -q -m "shm_channel: lock-free SPSC ring registry and shm backend workers"
fi

if ! git diff --quiet src/backend/libpq/be-secure.c; then
  git add src/backend/libpq/be-secure.c
  $G commit -q -m "shm_channel: route client I/O through the ring pair in shm mode"
fi

if ! git diff --quiet src/backend/postmaster/postmaster.c src/backend/storage/ipc/ipci.c; then
  git add src/backend/postmaster/postmaster.c src/backend/storage/ipc/ipci.c
  $G commit -q -m "shm_channel: postmaster bootstrap and BackendInitialize export"
fi

if ! git diff --quiet src/backend/utils/init/postinit.c; then
  git add src/backend/utils/init/postinit.c
  $G commit -q -m "shm_channel: authenticate shm workers via pg_hba in InitPostgres"
fi

git log --oneline -5

# 4) configure & build
if [ ! -f src/Makefile.global ]; then
  ./configure --prefix=/opt/pg16shm --without-readline --without-zlib --without-icu \
    > /home/ysyuan/pg16shm_build.log 2>&1
fi
make -j12 >> /home/ysyuan/pg16shm_build.log 2>&1 && make install >> /home/ysyuan/pg16shm_build.log 2>&1
echo "BUILD_RC=$?"
