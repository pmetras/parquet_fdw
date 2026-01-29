MODULE_big = parquet_fdw
OBJS = src/common.o src/reader.o src/exec_state.o src/parquet_impl.o src/parquet_fdw.o
PGFILEDESC = "parquet_fdw - foreign data wrapper for parquet"

SHLIB_LINK = -lm -lstdc++ -lparquet -larrow

EXTENSION = parquet_fdw
DATA = parquet_fdw--0.1.sql parquet_fdw--0.1--0.2.sql

# Generate names of test files

TEST_SQL_IN = $(sort $(wildcard test/sql/*.sql.in))
TEST_EXPECTED_IN = $(sort $(wildcard test/expected/*.out.in))

TEST_SQL = $(patsubst test/sql/%.sql.in,test/sql/%.sql,$(TEST_SQL_IN))
TEST_EXPECTED = $(patsubst test/expected/%.out.in,test/expected/%.out,$(TEST_EXPECTED_IN))

REGRESS = $(patsubst test/sql/%.sql.in,%,$(TEST_SQL_IN))

EXTRA_CLEAN = $(TEST_SQL) $(TEST_EXPECTED)
REGRESS_OPTS = --inputdir=test --outputdir=test

PG_CONFIG ?= pg_config

ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# parquet_impl.cpp requires C++ 11 and libarrow 10+ requires C++ 17
override PG_CXXFLAGS += -std=c++17 -O3

PGXS := $(shell $(PG_CONFIG) --pgxs)

# pass CCFLAGS (when defined) to both C and C++ compilers.
ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

include $(PGXS)

# XXX: PostgreSQL below 11 does not automatically add -fPIC or equivalent to C++
# flags when building a shared library, have to do it here explicitely.
ifeq ($(shell test $(VERSION_NUM) -lt 110000; echo $$?), 0)
	override CXXFLAGS += $(CFLAGS_SL)
endif

# PostgreSQL uses link time optimization option which may break compilation
# (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
#
# We need to use -Wno-register since C++17 raises an error if "register" keyword
# is used. PostgreSQL headers still uses the keyword, particularly:
# src/include/storage/s_lock.h.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes -Wno-register $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

# gcc 10.1 enables moutline-atomics per default on aarch64
# including the deprecated flag breaks compilation
GCC_GTEQ_1001 := $(shell expr `gcc -dumpfullversion -dumpversion | sed -e 's/\.\([0-9][0-9]\)/\1/g' -e 's/\.\([0-9]\)/0\1/g' -e 's/^[0-9]\{3,4\}$$/&00/'` \>= 100100)

ifeq "$(GCC_GTEQ_1001)" "1"
override CXXFLAGS := $(filter-out -moutline-atomics, $(CXXFLAGS))
endif

# XXX: a hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<

# PostgreSQL 15 dropped support of *.source files to generate tests using pg_regress.
# Because of that we generate paths in tests manually now.

installcheck: $(TEST_SQL) $(TEST_EXPECTED)

$(TEST_SQL): test/sql/%.sql: test/sql/%.sql.in
	sed 's,@abs_srcdir@,$(ROOT_DIR)/test,g' $< > $@

$(TEST_EXPECTED): test/expected/%.out: test/expected/%.out.in
	sed 's,@abs_srcdir@,$(ROOT_DIR)/test,g' $< > $@

# Debian package creation
# Usage: make debian [PG_CONFIG=/path/to/pg_config]
#
# Package naming: postgresql-<PGVER>-parquet-fdw_<VERSION>+<GITREV>_<ARCH>.deb
# Example: postgresql-17-parquet-fdw_0.2+g1a2b3c4_amd64.deb
#
# The git revision (or timestamp if not in a git repo) ensures each build
# produces a uniquely named package for tracking purposes.

PG_VERSION := $(shell $(PG_CONFIG) --version | sed 's/PostgreSQL \([0-9]*\).*/\1/')
PG_PKGLIBDIR := $(shell $(PG_CONFIG) --pkglibdir)
PG_SHAREDIR := $(shell $(PG_CONFIG) --sharedir)
PKG_NAME := postgresql-$(PG_VERSION)-parquet-fdw
PKG_BASE_VERSION := 0.2
# Use git short hash if available, otherwise use timestamp
PKG_GIT_REV := $(shell git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)
PKG_VERSION := $(PKG_BASE_VERSION)+g$(PKG_GIT_REV)
PKG_DIR := Debian/$(PKG_NAME)
PKG_LIBDIR := $(PKG_DIR)/usr/lib/postgresql/$(PG_VERSION)/lib
PKG_EXTDIR := $(PKG_DIR)/usr/share/postgresql/$(PG_VERSION)/extension
PKG_ARCH := $(shell dpkg --print-architecture)
PKG_FILENAME := $(PKG_NAME)_$(PKG_VERSION)_$(PKG_ARCH).deb

.PHONY: debian debian-clean

debian: all
	@echo "Building Debian package for PostgreSQL $(PG_VERSION)..."
	@echo "Package version: $(PKG_VERSION)"
	# Verify that installed files match the target PostgreSQL version
	@if [ -f "$(PG_PKGLIBDIR)/parquet_fdw.so" ]; then \
		INSTALLED_PATH="$$(dirname $(PG_PKGLIBDIR))"; \
		TARGET_PATH="/usr/lib/postgresql/$(PG_VERSION)"; \
		if [ "$$INSTALLED_PATH" != "$$TARGET_PATH" ]; then \
			echo "ERROR: Installed parquet_fdw.so is in $$INSTALLED_PATH"; \
			echo "       but package targets $$TARGET_PATH"; \
			echo "       Run 'make install PG_CONFIG=$(PG_CONFIG)' first."; \
			exit 1; \
		fi; \
	fi
	# Clean and create directory structure
	rm -rf $(PKG_DIR)
	mkdir -p $(PKG_DIR)/DEBIAN
	mkdir -p $(PKG_LIBDIR)/bitcode/parquet_fdw/src
	mkdir -p $(PKG_EXTDIR)
	# Copy shared library
	cp parquet_fdw.so $(PKG_LIBDIR)/
	# Copy LLVM bitcode files
	cp src/*.bc $(PKG_LIBDIR)/bitcode/parquet_fdw/src/
	# Create bitcode index (copy from installed location or generate)
	@if [ -f "$(PG_PKGLIBDIR)/bitcode/parquet_fdw.index.bc" ]; then \
		cp $(PG_PKGLIBDIR)/bitcode/parquet_fdw.index.bc $(PKG_LIBDIR)/bitcode/; \
	else \
		echo "Warning: bitcode index not found, run 'make install' first"; \
	fi
	# Copy extension files
	cp parquet_fdw.control $(PKG_EXTDIR)/
	cp parquet_fdw--*.sql $(PKG_EXTDIR)/
	# Generate control file
	@echo "Package: $(PKG_NAME)" > $(PKG_DIR)/DEBIAN/control
	@echo "Version: $(PKG_VERSION)" >> $(PKG_DIR)/DEBIAN/control
	@echo "Section: database" >> $(PKG_DIR)/DEBIAN/control
	@echo "Priority: optional" >> $(PKG_DIR)/DEBIAN/control
	@echo "Architecture: $(PKG_ARCH)" >> $(PKG_DIR)/DEBIAN/control
	@echo "Maintainer: Pierre Métras <p.metras@nfb.ca>" >> $(PKG_DIR)/DEBIAN/control
	@echo "Depends: postgresql-$(PG_VERSION), libarrow1800, libparquet1800" >> $(PKG_DIR)/DEBIAN/control
	@echo "Conflicts: postgresql-parquet-fdw" >> $(PKG_DIR)/DEBIAN/control
	@echo "Replaces: postgresql-parquet-fdw" >> $(PKG_DIR)/DEBIAN/control
	@echo "Description: Apache Parquet foreign data wrapper for PostgreSQL $(PG_VERSION)" >> $(PKG_DIR)/DEBIAN/control
	@echo " Read-only foreign data wrapper for Apache Parquet files." >> $(PKG_DIR)/DEBIAN/control
	@echo " ." >> $(PKG_DIR)/DEBIAN/control
	@echo " Features:" >> $(PKG_DIR)/DEBIAN/control
	@echo "  - Query Parquet files as PostgreSQL foreign tables" >> $(PKG_DIR)/DEBIAN/control
	@echo "  - Support for file globbing and multiple files" >> $(PKG_DIR)/DEBIAN/control
	@echo "  - Hive-style partitioning with partition pruning" >> $(PKG_DIR)/DEBIAN/control
	@echo "  - Parallel query execution" >> $(PKG_DIR)/DEBIAN/control
	@echo " ." >> $(PKG_DIR)/DEBIAN/control
	@echo " Built from git revision: $(PKG_GIT_REV)" >> $(PKG_DIR)/DEBIAN/control
	@echo " See https://github.com/pmetras/parquet_fdw" >> $(PKG_DIR)/DEBIAN/control
	# Build the package with versioned filename
	dpkg-deb --build --root-owner-group $(PKG_DIR)
	mv Debian/$(PKG_NAME).deb Debian/$(PKG_FILENAME)
	@echo ""
	@echo "Package created: Debian/$(PKG_FILENAME)"
	@echo "  PostgreSQL version: $(PG_VERSION)"
	@echo "  Package version: $(PKG_VERSION)"
	@echo "  Architecture: $(PKG_ARCH)"

debian-clean:
	rm -rf Debian/postgresql-*-parquet-fdw
	rm -f Debian/postgresql-*-parquet-fdw*.deb
