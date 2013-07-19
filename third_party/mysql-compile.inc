# This script contains the autogen/configure/make part of the
# build. It is included in several executables (build, deb, mpm, etc)
# and it assumes a whole set of environmental variables are properly
# set.

VT_MYSQL_ROOT=$VTROOT/dist/mysql

# Autogen

${ACLOCAL}
autoheader
libtoolize --automake --force --copy
${AUTOMAKE} --foreign --add-missing --copy
autoconf

# Configure
pushd ./

cd ${BUILD_DIR}
set -x
${SRC_DIR}/configure \
  --host=${GNU_TARGET} \
  --enable-assembler \
  --enable-local-infile \
  --enable-profiling \
  --with-big-tables \
  --without-manager \
  --with-extra-charsets=all \
  --with-mysqld-user=mysql \
  --with-unix-socket-path=/var/lib/mysql/mysql.sock \
  --with-pic \
  --prefix=$VT_MYSQL_ROOT \
  --exec-prefix=$VT_MYSQL_ROOT \
  --libexecdir=$VT_MYSQL_ROOT/sbin \
  --libdir=$VT_MYSQL_ROOT/lib \
  --datadir=$VT_MYSQL_ROOT/share \
  --localstatedir=/var/lib/mysql \
  --sysconfdir=/etc \
  --infodir=$VT_MYSQL_ROOT/share/info \
  --includedir=$VT_MYSQL_ROOT/include \
  --enable-thread-safe-client \
  --with-plugins=innodb_plugin,googlestats \
  --with-lzo2-dir=bundled \
  --with-ncurses-dir=bundled \
  --with-readline \
  --with-perftools-dir=bundled \
  ${enable_perftools_profiling} \
  --enable-perftools-tcmalloc \
  ${enable_minimal} \
  --with-ssl=/usr \
  --with-comment="Google MySQL ${MYSQL_VERSION} build ${MYSQL_RELEASE}" \
  --with-zlib-dir=bundled \
  --with-server-suffix=${server_suffix} \
  --without-embedded-server \
  --without-vio \
  ${with_debug} \
  --without-docs \
  --without-man \
  CC=${CC} CPP=${CPP} CXX=${CXX} LD=${LD} RANLIB=${RANLIB} AR=${AR} \
  CPPFLAGS="${CPPFLAGS}" \
  LDFLAGS="${LDFLAGS} ${profile_lib}" \
  STRIP="/bin/true" \
  CFLAGS="${CFLAGS} ${profile_opt} ${ncurses_inc}" \
  CXXFLAGS="${CXXFLAGS} ${profile_opt} ${ncurses_inc}" \
  GETCONF="${GETCONF_BIN}"
set -x

# Build

make ${MAKEFLAGS}

popd
