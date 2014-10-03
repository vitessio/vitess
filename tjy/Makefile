# -----------------------------------------------------------------------------
#     G N U    M A K E   S C R I P T   S P E C I F I C A T I O N
# -----------------------------------------------------------------------------
# NAME
#
#  Makefile - To automate vitess software build, packaging .
#
#
#
# REVISION HISTORY
#     09/13/2014   T.J. Yang init.
#
# USAGE
#     make [packagename-version|buildworlds|upload=1] 
#
#
#
# DESCRIPTION
# 
#
# 
# TODOs
#  1. Improve Makefile syntax.

# ---------------------------- CONSTANT DECLARATION ---------------------------
CAT=/bin/cat
ECHO=/bin/echo
DIST_NAME=moto
# system type indetifcation
SYSTYPE=./mklb/systype
# Package Distribution Name
PKG_DIST=OpenIndiana
PKGUTIL=/opt/TWWfsw/bin/pkg-inst
oiuser="oi"
SB_DB=/opt/${DIST_NAME}/.sb
SBFX=/opt/${DIST_NAME}/.sb
VARDIST=/vart/opt/${DIST_NAME}
#BUILD_DIR=/opt/build/${DIST_NAME}
BUILD_DIR=/opt/build
BUILD_DB=$(shell pwd)/build/.sb
# local TWW 9.0 dist src path
SOURCE_DIR=$(shell pwd)/9.1/src
# Xymon related src directory path.
XYMON_SOURCE_DIR=../${DIST_NAME}addon
# OIADDON related src directory path.
BUILD_DB=$(shell pwd)/build/.sb
ADDON_DIR=$(shell pwd)/9.1addon
ADDON_DIR2=$(shell pwd)
MKFILES=osmkfiles
BOOKS="book" 
GRAPH="doc/dia" 
# 
PERL=/usr/bin/perl
SUDO=/usr/bin/sudo
MKDIR=/bin/mkdir
CHOWN=/usr/sbin/chown
TAR=/bin/tar
#RPM=/usr/bin/rpm
RPM=/bin/rpm
APT_GET=/usr/bin/apt-get
UNZIP=/usr/bin/unzip
YUM=/usr/bin/yum
RM=/bin/rm -rf 
SUDORM=${SUDO} ${RM} 
BDIR="--builddir=/opt/build"
# what : a simple way to give option "gmake" to specify either to 
#        upload the package to repository server or not. also to
#       specify which TWW distribution to build. 

DISTNAME=moto
TWWSB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb  --builddir=/opt/build  --install-base=/opt/TWWfsw
#DIST_PREFIX=$(shell pwd)/dist/${DISTNAME}/cd
#DIST_BASE=/opt/dist/${DISTNAME}/cd/dists
DIST_BASE=/opt/dist/cd
PACKAGE_BUILD=/opt/TWWfsw/bin/pb 
GPD=/opt/TWWfsw/bin/gen-pkg-db -b /opt/${DIST_NAME}
#DIST_PREFIX=$(shell pwd)/dist/${DIST_NAME}/cd
DIST_PREFIX=/opt/dist/${DIST_NAME}/cd
PACKAGE_UPLOAD_PATH=$(DIST_PREFIX)/dists/${PKG_DIST}/packages
BPFX=/opt/build
IPFX=/opt/${DIST_NAME}
ISUFFIX=${DIST_NAME}
INSTALL_PREFIX=/opt/$(ISUFFIX)
SOURCE_DIR=9.1/src
TS=`date`

ifeq ($(localsrc),1) 
 TWWURL=ftp://192.168.1.32/tww/${SOURCE_DIR}
else  
 TWWURL=ftp://support.thewrittenword.com/dists/${SOURCE_DIR}
endif 

ADDON_SRC=$(shell pwd)/9.1addon
M5S=/opt/TWWfsw/sbutils13/lib/aux/coreutils/bin/gmd5sum
# 
RSYNC=/usr/bin/rsync -azpv
GUPLOAD=/home2/tjyang/psp/lib/googlecode_upload.py -p psp
ZIP=zip -9
GIT=/usr/bin/git
HG=/usr/bin/hg
WGET=/usr/bin/wget
CURL=/usr/bin/curl  --create-dirs
PKG_INST=/opt/TWWfsw/bin/pkg-inst
CP=/bin/cp
# without in .phony books will not act
.PHONY: books books-clean twwtools centos-rpms
all: help

help: 
	@$(ECHO) init: to configure build needed directory and setting.
	@$(ECHO) list : list out buildables packages names.
	@$(ECHO) help : this help message.
	@$(ECHO) upload=1: to create and upload package to app depot.
	@$(ECHO) localsrc=1: get src.sb from local repository server.
	@$(ECHO) buildworlds: built package with both sb and pb
	@$(ECHO) "Example: "
	@$(ECHO) "  1. make hello-2.7-clean : to clean the current build"
	@$(ECHO) "  2. make hello-2.7 : to build and package software"
	@$(ECHO) "  3. make hello-2.7 upload=1 : to upload the package to package depot"
	@$(ECHO) "  4. make {books|books-clean} : generate books using texlive."
	@$(ECHO) systype = $(shell  $(SYSTYPE))

ifeq ($(shell  $(SYSTYPE)),sparc-sun-solaris2.11)
SB=pfexec /opt/TWWfsw/sbutils13/bin/sb
ARCHI=sparc-sun-solaris2.11
include ${MKFILES}/Makefile.solaris.common
include ${MKFILES}/Makefile.perl-modules
else

ifeq ($(shell  $(SYSTYPE)),i386-pc-solaris2.11)
ARCHI=i386-pc-solaris2.11
GIT=/usr/bin/git
include ${MKFILES}/Makefile.solaris.common
else

ifeq ($(shell  $(SYSTYPE)),x86_64-redhat-linuxe6)
CHOWN=/bin/chown
PPM=/bin/rpm
#SB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb -uCBi 
SB=/opt/TWWfsw/sbutils13/bin/sb -uCBi 
ARCHI=x86_64-redhat-linuxe6
PB=/opt/TWWfsw/pbutils11/bin/pb
include ${MKFILES}/Makefile.${ARCHI}

# mkdir /opt/build
twwtools: getpkg untar installpkgutil getsb getpb installsbpb 
getpkg	: pkgutils-1.6.7.rpm.tar

pkgutils-1.6.7.rpm.tar: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pkgutils-1.6.7.rpm.tar
untar	: 
	${TAR} xf pkgutils-1.6.7.rpm.tar
installpkgutil: $(PKGUTIL)


$(PKGUTIL):
	(for i in TWWpkg* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)	

getsb	: sbutils-1.3.1-1.rpm.pkg-inst

sbutils-1.3.1-1.rpm.pkg-inst: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/sbutils-1.3.1-1.rpm.pkg-inst

getpb	: pbutils-1.1.22-2.rpm.pkg-inst

pbutils-1.1.22-2.rpm.pkg-inst:
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pbutils-1.1.22-2.rpm.pkg-inst

gettwwrpm: rpm-4.2.rpm.tar

rpm-4.2.rpm.tar: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/rpm-4.2.rpm.tar

untar-rpm-4.2.rpm.tar: 
	${TAR} xf rpm-4.2.rpm.tar


install-rpm:
	(for i in TWWrpm* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)	



#installsb : 
#	${UNZIP} -d /tmp sbutils-1.3.1-1.rpm.pkg-inst
#	(cd /tmp;${UNZIP} -d /tmp/t data.zip)
#	(cd /tmp/t;for i in TWWsbutil* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)
##	($(SUDO) $(PERL) -pi -e 's!\/opt\/build!build!' /opt/TWWfsw/sbutils13/etc/sbutils.conf)
#

installsbpb : 
	${SUDO} ${PKGUTIL}  --systype=x86_64-redhat-linuxe6 sbutils-1.3.1-1.rpm.pkg-inst pbutils-1.1.22-2.rpm.pkg-inst

#	($(SUDO) $(PERL) -pi -e 's!\/opt\/build!build!' /opt/TWWfsw/sbutils13/etc/sbutils.conf)



else

ifeq ($(shell  $(SYSTYPE)),x86_64-centos-linuxe6)
SB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb -uCBi $(BDIR)
PB=/opt/TWWfsw/pbutils11/bin/pb
ARCHI=x86_64-centos-linuxe6
CPU:=x86_64
DIST_PREFIX=$(DIST_BASE)
RPM=/bin/rpm
CHOWN=/bin/chown
include ${MKFILES}/Makefile.${ARCHI}

twwtools: getpkg  /opt/TWWfsw/bin/pkg-inst  getsb /opt/TWWfsw/bin/sb  getpb /opt/TWWfsw/bin/pb gettwwrpm

gettwwrpm: rpm-4.2.rpm.tar

rpm-4.2.rpm.tar: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/rpm-4.2.rpm.tar

getpkg	: pkgutils-1.6.7.rpm.tar

pkgutils-1.6.7.rpm.tar: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pkgutils-1.6.7.rpm.tar

installpkgutil: /opt/TWWfsw/bin/pkg-inst

/opt/TWWfsw/bin/pkg-inst:
	${TAR} xf pkgutils-1.6.7.rpm.tar
	(for i in TWWpkg* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)
	${SUDO} ${CP}  lib/systype.pkgutil16.tww  /opt/TWWfsw/pkgutils16/lib/systype
getsb	: sbutils-1.3.1-1.rpm.pkg-inst

sbutils-1.3.1-1.rpm.pkg-inst: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/sbutils-1.3.1-1.rpm.pkg-inst
/opt/TWWfsw/bin/sb: 
	${SUDO} ${PKGUTIL} sbutils-1.3.1-1.rpm.pkg-inst
getpb	: pbutils-1.1.22-2.rpm.pkg-inst

pbutils-1.1.22-2.rpm.pkg-inst:
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pbutils-1.1.22-2.rpm.pkg-inst

/opt/TWWfsw/bin/pb: 
	${SUDO} ${PKGUTIL} pbutils-1.1.22-2.rpm.pkg-inst

else

ifeq ($(shell  $(SYSTYPE)),x86_64-GCEL-centos6)
SB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb $(BDIR)
PB=/opt/TWWfsw/pbutils11/bin/pb
ARCHI=x86_64-centos-linuxe6
CPU:=x86_64
DIST_PREFIX=$(DIST_BASE)
RPM=/bin/rpm
CHOWN=/bin/chown
include ${MKFILES}/Makefile.${ARCHI}

twwtools: getpkg untar installpkgutil getsb installsb
getpkg	: pkgutils-1.6.7.rpm.tar

pkgutils-1.6.7.rpm.tar: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pkgutils-1.6.7.rpm.tar
untar	: 
	${TAR} xf pkgutils-1.6.7.rpm.tar
installpkgutil: 
	(for i in TWWpkg* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)

getsb	: sbutils-1.3.1-1.rpm.pkg-inst
sbutils-1.3.1-1.rpm.pkg-inst: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/sbutils-1.3.1-1.rpm.pkg-inst
installsb : 
	${UNZIP} -d /tmp sbutils-1.3.1-1.rpm.pkg-inst
	(cd /tmp;${UNZIP} -d /tmp/t data.zip)
	(cd /tmp/t;for i in TWWsbutil* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)
#	($(SUDO) $(PERL) -pi -e 's!\/opt\/build!build!' /opt/TWWfsw/sbutils13/etc/sbutils.conf)


else

ifeq ($(shell  $(SYSTYPE)),i686-debian-6.0.5)
ARCHI=i686-debian-6.0.5
else

ifeq ($(shell  $(SYSTYPE)),x86_64-ubuntu-12.04)
SB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb
ARCHI=$(shell  $(SYSTYPE))
include ${MKFILES}/Makefile.${ARCHI}

twwtools: getpkg untar installpkgutil getsb installsb
getsb	: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/sbutils-1.3.1-1.rpm.pkg-inst
installsb : 
	${UNZIP} -d /tmp sbutils-1.3.1-1.rpm.pkg-inst
	(cd /tmp;${UNZIP} -d /tmp/t data.zip)
	(cd /tmp/t;for i in TWWsbutil* ;do $(SUDpO) $(RPM) --nodeps -ivh $$i; done)
#	($(SUDO) $(PERL) -pi -e 's!\/opt\/build!build!' /opt/TWWfsw/sbutils13/etc/sbutils.conf)
getpkg	: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pkgutils-1.6.7.rpm.tar
untar	: 
	${TAR} xf pkgutils-1.6.7.rpm.tar
installpkgutil: 
	(for i in TWWpkg* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)
getrpm:
	$(SUDO) $(APT_GET) install rpm bison -y

else

ifeq ($(shell  $(SYSTYPE)),i686-ubuntu-12.04)
SB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb
ARCHI=i686-ubuntu-12.04
include ${MKFILES}/Makefile.${ARCHI}

else

ifeq ($(shell  $(SYSTYPE)),x86_64-ubuntu-12.10)
SB=${SUDO} /opt/TWWfsw/sbutils13/bin/sb
CPU:=x86_64
ARCHI=$(shell  $(SYSTYPE))
include ${MKFILES}/Makefile.${ARCHI}
#sbutils-1.3.1-1.rpm.pkg-inst
#ftp://support.thewrittenword.com/dists/9.0/support/x86_64-redhat-linuxe5/pbutils-1.1.22-1.rpm.pkg-inst
twwtools: get untar installpkgutil
get	: 
	${WGET} ftp://support.thewrittenword.com/dists/9.1/support/x86_64-redhat-linuxe6/pkgutils-1.6.7.rpm.tar
untar	: 
	${TAR} xf pkgutils-1.6.7.rpm.tar
installpkgutil: 
	(for i in TWWpkg* ;do $(SUDO) $(RPM) --nodeps -ivh $$i; done)
getrpm:
	$(APT_GET) install rpm bison -y

else

ifeq ($(shell  $(SYSTYPE)),x86_64-apple-darwin11.4.0)
SB=${SUDO} /opt/bd/sbutils128/bin/sb
ARCHI=x86_64-apple-darwin11.4.0
include ${MKFILES}/Makefile.${ARCHI}
else

ifeq ($(shell  $(SYSTYPE)),x86_64-apple-darwin11.4.2)
SB=${SUDO} /opt/bd/sbutils128/bin/sb --builddir=/opt/build
ARCHI=x86_64-apple-darwin11.4.2
include ${MKFILES}/Makefile.${ARCHI}
else

ifeq ($(shell  $(SYSTYPE)),x86_64-apple-darwin12.2.1) 
HG=/usr/local/bin/hg
SB=${SUDO} /opt/bd/sbutils128/bin/sb --builddir=/opt/build
ARCHI=x86_64-apple-darwin12.2.1 
include ${MKFILES}/Makefile.${ARCHI}
else


ifeq ($(shell $(SYSTYPE)),x86_64-unknown-linux-gnu)
ARCHI=x86_64-unknown-linux-gnu
BuildDir=${TMP}/build
PKGNAME=${program_name}-${version}.pkg
build_name=${program_name}-${version}
install_name=${program_name}130
GMAKE=/usr/bin/make
# configure distcc 
SET_DISTCC_HOSTS=DISTCC_HOSTS='imac'
SETCC=CC='/usr/bin/distcc /usr/bin/gcc'
CC=/usr/bin/gcc
GPATCH=/usr/bin/patch
BZIP2=/bin/bzip2
GZIP=/bin/gzip
CFLAGS="-O2"
CC_LD_RT="-Wl,-rpath," 
LDFLAGS=""
CONFIGURE=./configure 
else
cantbuild:
	@$(ECHO) "Error:  $(shell  $(SYSTYPE)) target OS is not supported."

endif # end of sparc-sun-solaris2.11
endif # end of i386-pc-solaris2.11
endif # end of x86_64-redhat-linuxe6
endif # end of x86_64-centos-linuxe6
endif # end of x86_64-GCEL-centos6
endif # end of i686-debian-6.0.5
endif # end of x86_64-ubuntu-12.04
endif # end of i686-ubuntu-12.04
endif # end of x86_64-ubuntu-12.10
endif # end of x86_64-apple-darwin11.4.0
endif # end of x86_64-apple-darwin11.4.2
endif # end of x86_64-apple-darwin12.2.1 
endif # end of x86_64-linux-unknown



list:
	@ls $(SOURCE_DIR) $(ADDON_DIR)
tww2oi:
	$(ECHO) "for i in 9.0/src/*/sb-db.xml  ; do cp  $i $i.${DIST_NAME} ; done"
	$(ECHO) "for i in 9.0/src/*/pb-db.xml  ; do cp  $i $i.${DIST_NAME} ; done"
	$(ECHO) "perl -pi -e 's!TWW!${DIST_NAME}!' 9.0/src/*/pb-db.xml.${DIST_NAME}"
	$(ECHO) "perl -pi -e 's!\<\!DOCTYPE.*!!' 9.0/src/*/sb-db.xml.${DIST_NAME}"
init : motoprework 
	mkdir -p /opt/${DIST_NAME} 

# Global functions 
# $(call update-depot-pkg-db,pkgname)
# $1 is the first argument.

#WHAT: for Xymon related pb-db.xml.oi
define update-xymon-depot-pkg-db
	${RSYNC} ${PACKAGE_UPLOAD_PATH}/${ARCHI}/pkg-db.xml /tmp;
	${PERL} -pi -e 's!</packages>!!' /tmp/pkg-db.xml
	${GPD}  ${XYMON_SOURCE_DIR}/$1/pb-db.xml.${DIST_NAME} > /tmp/tmp.xml
	${PERL} -pi -e 's!^</packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<\?xml version="1.0"\?>!!' /tmp/tmp.xml
	$(CAT) /tmp/tmp.xml  >> /tmp/pkg-db.xml;rm -f /tmp/tmp.xml
	$(ECHO) "</packages>" >> /tmp/pkg-db.xml
	${RSYNC} /tmp/pkg-db.xml ${PACKAGE_UPLOAD_PATH}/${ARCHI}/
endef


# function: upload-pkg-inst
# Usage: $(call upload-pkg-inst,summary,pkgname)
# $1 is the first argument.

#WHAT: for Xymon related pb-db.xml.oi
define upload-pkg-inst
	cd ${DIST_PREFIX}/${CPU};${GUPLOAD} -s $1 $2
endef


#WHAT: for TWW 9.1 src related pb-db.xml.${DIST_NAME}
define update-depot-pkg-db
	${RSYNC} ${PACKAGE_UPLOAD_PATH}/${ARCHI}/pkg-db.xml /tmp;
	${PERL} -pi -e 's!</packages>!!' /tmp/pkg-db.xml
	${GPD}  ${SOURCE_DIR}/$1/pb-db.xml.${DIST_NAME} > /tmp/tmp.xml
	${PERL} -pi -e 's!^</packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<\?xml version="1.0"\?>!!' /tmp/tmp.xml
	$(CAT) /tmp/tmp.xml  >> /tmp/pkg-db.xml;rm -f /tmp/tmp.xml
	$(ECHO) "</packages>" >> /tmp/pkg-db.xml
	${RSYNC} /tmp/pkg-db.xml ${PACKAGE_UPLOAD_PATH}/${ARCHI}/
endef


#define update-depot-pkg-db-moto
#	${RSYNC} ${PACKAGE_UPLOAD_PATH}/${ARCHI}/pkg-db.xml /tmp;
#	${PERL} -pi -e 's!</packages>!!' /tmp/pkg-db.xml
#	${GPD}  ${SOURCE_DIR}/$1/pb-db.xml.${DIST_NAME} > /tmp/tmp.xml
#	${PERL} -pi -e 's!^</packages>!!' /tmp/tmp.xml
#	${PERL} -pi -e 's!^<packages>!!' /tmp/tmp.xml
#	${PERL} -pi -e 's!^<\?xml version="1.0"\?>!!' /tmp/tmp.xml
#	$(CAT) /tmp/tmp.xml  >> /tmp/pkg-db.xml;rm -f /tmp/tmp.xml
#	$(ECHO) "</packages>" >> /tmp/pkg-db.xml
#	${RSYNC} /tmp/pkg-db.xml ${PACKAGE_UPLOAD_PATH}/${ARCHI}/
#endef

define update-depot-pkg-db-moto
	${GPD}  ${SOURCE_DIR}/$1/pb-db.xml.${DIST_NAME} > /tmp/tmp.xml
	${PERL} -pi -e 's!^</packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<\?xml version="1.0"\?>!!' /tmp/tmp.xml
	$(CAT) /tmp/tmp.xml  >> /tmp/pkg-db.xml;rm -f /tmp/tmp.xml
	$(ECHO) "</packages>" >> /tmp/pkg-db.xml
	${RSYNC} /tmp/pkg-db.xml ${PACKAGE_UPLOAD_PATH}/${ARCHI}/
endef


#WHAT:
#WHY:
define update-depot-pkg-db-${DIST_NAME}addon
	${RSYNC} ${PACKAGE_UPLOAD_PATH}/${ARCHI}/pkg-db.xml /tmp;
	${PERL} -pi -e 's!</packages>!!' /tmp/pkg-db.xml
	${GPD}  ${${DIST_NAME}ADDON_SRC}/$1/pb-db.xml.${DIST_NAME} > /tmp/tmp.xml
	${PERL} -pi -e 's!^</packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<\?xml version="1.0"\?>!!' /tmp/tmp.xml
	$(CAT) /tmp/tmp.xml  >> /tmp/pkg-db.xml;rm -f /tmp/tmp.xml
	$(ECHO) "</packages>" >> /tmp/pkg-db.xml
	${RSYNC} /tmp/pkg-db.xml ${PACKAGE_UPLOAD_PATH}/${ARCHI}/
endef


define update-depot-pkg-db-2.6
	${RSYNC} ${PACKAGE_UPLOAD_PATH}/${ARCHI}/pkg-db.xml /tmp;
	${PERL} -pi -e 's!</packages>!!' /tmp/pkg-db.xml
	${GPD}  ${SOURCE_DIR}/$1/pb-db.xml.${DIST_NAME}.2.6 > /tmp/tmp.xml
	${PERL} -pi -e 's!^</packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<packages>!!' /tmp/tmp.xml
	${PERL} -pi -e 's!^<\?xml version="1.0"\?>!!' /tmp/tmp.xml
	$(CAT) /tmp/tmp.xml  >> /tmp/pkg-db.xml;rm -f /tmp/tmp.xml
	$(ECHO) "</packages>" >> /tmp/pkg-db.xml
	${RSYNC} /tmp/pkg-db.xml ${PACKAGE_UPLOAD_PATH}/${ARCHI}/
endef


#WHAT: 
uploadl:
	$(RSYNC) -azpv . rsync://192.168.1.2/${DIST_NAME}pkg/

uploadr:
	$(RSYNC) -azpv . rsync://c-71-57-114-187.hsd1.il.comcast.net/${DIST_NAME}pkg/

downloadr:
	$(RSYNC) -azpv  rsync://192.168.1.2/${DIST_NAME}pkg .

# pkgdir = "/home/${DIST_NAME}/${DIST_NAME}pkg/dist/${DIST_NAME}/cd"
# install-base = "/opt/${DIST_NAME}"
#   PATH = "/opt/TWWfsw/sbutils13/lib/aux/bash/bin:\
# /opt/TWWfsw/sbutils13/lib/aux/bzip2/bin:\


twwprework: 
	${SUDO} ${MKDIR} -p /opt/dist/${DIST_NAME}/cd \
        /opt/build /opt/${DIST_NAME} /opt/dist/${DIST_NAME}/cd/dists \
        $(shell pwd)/dist/${DIST_NAME}/cd/dists/OpenIndiana/packages/i386-pc-solaris2.11 \
        $(shell pwd)/dist/${DIST_NAME}/cd/dists/OpenIndiana/packages/sparc-sun-solaris2.11
	${SUDO} ${CHOWN} -R `id -u`  ./dist ./build /opt/${DIST_NAME}
	${SUDO} ${PERL} -pi -e 's!"/opt/build"!"/opt/build"!' /opt/TWWfsw/sbutils13/etc/sbutils.conf
	${SUDO} ${PERL} -pi -e 's!"/opt/TWWfsw"!"/opt/${DIST_NAME}"!' /opt/TWWfsw/sbutils13/etc/sbutils.conf
	${SUDO} ${PERL} -pi -e 's!"/opt/build"!"/opt/build"!' /opt/TWWfsw/pbutils11/etc/pbutils.conf
#	${SUDO} ${PERL} -pi -e 's!"./dist/cd"!"./dist/${DIST_NAME}/cd"!' /opt/TWWfsw/pbutils11/etc/pbutils.conf
	${SUDO} ${PERL} -pi -e 's!"/opt/TWWfsw"!"/opt/${DIST_NAME}"!' /opt/TWWfsw/pbutils11/etc/pbutils.conf


motoprework: twwtools
	${SUDO} ${MKDIR} -p /opt/build /opt/${DIST_NAME} \
        /opt/dist/${DIST_NAME}/cd/dists/x86_64
	${SUDO} ${CHOWN} -R `id -u`  /opt/dist /opt/build /opt/${DIST_NAME}
	${SUDO} ${PERL} -pi -e 's!"^build-dir\s=\s.*!"/opt/build"!' /opt/TWWfsw/sbutils13/etc/sbutils.conf
	${SUDO} ${PERL} -pi -e 's!"/opt/TWWfsw"!"/opt/${DIST_NAME}"!' /opt/TWWfsw/sbutils13/etc/sbutils.conf
	${SUDO} ${PERL} -pi -e 's!"/opt/build"!"/opt/build"!' /opt/TWWfsw/pbutils11/etc/pbutils.conf
#	${SUDO} ${PERL} -pi -e 's!"/opt/dist/cd"!"/opt/dist/${DIST_NAME}/cd"!' /opt/TWWfsw/pbutils11/etc/pbutils.conf
	${SUDO} ${PERL} -pi -e 's!"/opt/TWWfsw"!"/opt/${DIST_NAME}"!' /opt/TWWfsw/pbutils11/etc/pbutils.conf
	${SUDO} ${CP}  lib/systype.sb.tww /opt/TWWfsw/sbutils13/lib/systype
	${SUDO} ${CP}  lib/systype.pb.tww /opt/TWWfsw/pbutils11/lib/systype
	${SUDO} ${CP}  lib/systype.pkgutil16.tww /opt/TWWfsw/pkgutils16/lib/systype
#	${SUDO} ${YUM} install bison flex gcc-c++ ncurses-devel gmp-devel glibc-devel.i686 ppl-devel -y

#4doc := texlive texlive-latex texlive-xetex texlive-utils telive-east-asian graphviz dia graphviz inkscape
4doc := graphviz dia graphviz inkscape

install-tl:
	${WGET} http://ctan.sharelatex.com/tex-archive/systems/texlive/tlnet/install-tl-unx.tar.gz
	${TAR} xzf install-tl-unx.tar.gz

EPEL:
	${SUDO} ${RPM} -ivh http://mirrors.servercentral.net/fedora/epel/6/i386/epel-release-6-8.noarch.rpm
centos-rpms: 
	${SUDO} ${YUM} install ${4doc} bison flex gcc-c++ ncurses-devel gmp-devel glibc-devel glibc-devel.i686 ppl-devel libX11-devel -y
ipsprework: 
	pfexec pkg install  lftp git gnu-make wget rsync zip developer/object-file header-math imake makedepend

clean:
	rm -rf build/* *~ 
space:
	@du -ks . 

cleansrc:
	find ${SOURCE_DIR} ${ADDON_SRC} -type f -name *.gz

cleanbuildsrc:
	${SUDO} rm -rf ${BUILD_DIR}/* ${SB_DB}/* ${VARDIST}
gitpush:
	($(GIT) commit -a -m "makefile update at ${TS} " )
	($(GIT) push origin master )

upstream-update:
	($(GIT) fetch upstream)
	($(GIT)  merge upstream/master)
	($(GIT) push origin master )

hgpush:
	( $(HG) commit -m "makefile update" )
	( $(HG) push)

gitpull: 
	($(GIT) pull )

books:
	(for i in "$(BOOKS)" ;do  $(MAKE) -C $$i ; done)	

diaclean:
	(for i in "$(GRAPH)" ;do  $(MAKE) -C $$i clean; done)	

dia:
	(for i in "$(GRAPH)" ;do  $(MAKE) -C $$i all  ; done)	

books-clean:
	@(for i in "$(BOOKS)" ;do  $(MAKE) -C $$i clean ; done)	


buildworlds:  gcc-4.4.6 perl-5.12.2 python-2.7.2 gcc-4.7.2 ruby-1.8.7 sbutils-1.3.1 pbutils-1.2.0 pkgutils-1.6.0
	@echo "Done building $@"
